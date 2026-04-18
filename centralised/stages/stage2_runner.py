"""Stage 2 streaming runner."""
from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, Tuple

from centralised.metrics_export import StageMetrics, mark_streaming_abort
from centralised.utils import greedy_slot_assignment as _greedy_slot_assignment

Vec3 = Tuple[float, float, float]

def run_stage2_streaming_impl(mission: Any) -> StageMetrics:
    """Stage2: continuous 10 Hz streaming through the wall windows.

    Architecture mirrors ``run_stage1_streaming`` + ``run_stage4``:

      Phase 0 -- Prime PositionMotion with each drone's current XY at
                 cruise_height so the first slew target isn't a teleport.
      Phase 1 -- Rally-in-place in the plan's initial formation (columnn)
                 with greedy nearest-neighbour slot assignment, to
                 prevent the takeoff-cluster crossing-paths problem.
      Phase 2 -- Ferry slew from current leader XY to the plan's entry
                 waypoint (still in columnn).
      Phase 3 -- Streaming cruise along the polyline returned by
                 ``plan_stage2()`` at a fixed linear speed. Formation
                 follows the per-segment formation name, blended over
                 ``transition_s`` seconds at each change.
      Phase 4 -- Hover at the final pose for 1 s.

    Along the way we sample per-tick telemetry identical in spirit to
    stage1 (formation RMSE, min pair distance, pair-collision ticks)
    PLUS a stage2-specific obstacle-collision detector that counts
    ticks where any drone crosses a wall outside of its gap.
    """
    stage_name = "stage2"
    stage = mission.scenario.stage2
    metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
    metrics.start_time = time.time()
    if stage is None:
        metrics.end_time = time.time()
        mission.metrics[stage_name] = metrics
        return metrics

    if not mission.dry_run and not mission._ensure_motion_ref_loaded():
        mission._log("[stage2] motion_ref load failed; abort")
        return mark_streaming_abort(metrics, mission, "motion_ref unavailable")

    plan = mission.planner.plan_stage2()
    if not plan.waypoints:
        mission._log("[stage2] empty plan; abort")
        return mark_streaming_abort(metrics, mission, "empty plan")

    cruise_z = mission.planner.cruise_height
    default_spacing = mission.controller.default_spacing

    rate_hz = 10.0
    dt = 1.0 / rate_hz
    target_speed = 0.30   # leader linear speed along polyline (m/s)
    slew_speed = 0.40     # m/s during ferry
    rally_duration = 2.5
    hover_duration = 1.0
    transition_s = 1.5    # formation blend duration between segments
    twist_slew = [0.6, 0.6, 0.5]
    twist_cruise = [0.6, 0.6, 0.5]
    twist_rally = [0.4, 0.4, 0.3]
    PAIR_SAFETY_M = 0.30
    PAIR_PHYSICAL_M = 0.15
    DRONE_RADIUS = 0.12

    from centralised.formations import get_formation_offsets as _get_offsets
    from centralised.utils import rotate_xy as _rotate_xy

    # ---- Leader current position (used for rally / ferry start)
    try:
        if not mission.dry_run:
            lp = mission.drones[0].position
            lp_x, lp_y = float(lp[0]), float(lp[1])
        else:
            lp_x, lp_y = 0.0, 0.0
    except Exception:
        lp_x, lp_y = 0.0, 0.0

    entry = plan.waypoints[0]
    entry_x, entry_y, entry_z = entry
    if math.hypot(entry_x - lp_x, entry_y - lp_y) > 0.05:
        slew_yaw = math.atan2(entry_y - lp_y, entry_x - lp_x)
    else:
        slew_yaw = 0.0

    # ---- Greedy slot assignment for the initial columnn rally
    initial_form = plan.formation_names[0].lower() if plan.formation_names else "columnn"
    rally_offsets_local = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
    rally_slots_global_xy: List[Tuple[float, float]] = []
    for off in rally_offsets_local:
        dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
        rally_slots_global_xy.append((lp_x + dxg, lp_y + dyg))

    follower_positions_now: List[Tuple[float, float]] = []
    if not mission.dry_run:
        for d in mission.drones[1:]:
            try:
                p = d.position
                follower_positions_now.append((float(p[0]), float(p[1])))
            except Exception:
                follower_positions_now.append((lp_x, lp_y))
    else:
        follower_positions_now = [(lp_x + i * 0.3, lp_y) for i in range(4)]

    slot_to_follower = _greedy_slot_assignment(follower_positions_now, rally_slots_global_xy)
    drone_to_slot: List[int] = [0] * 4
    for slot_i, drone_idx in enumerate(slot_to_follower):
        if 0 <= drone_idx < 4:
            drone_to_slot[drone_idx] = slot_i

    # ---- Build polyline segments from plan.waypoints[0..N-1]
    segs: List[Dict[str, object]] = []
    total_len = 0.0
    for i in range(1, len(plan.waypoints)):
        a = plan.waypoints[i - 1]
        b = plan.waypoints[i]
        seg_len = math.hypot(b[0] - a[0], b[1] - a[1]) + 0.5 * abs(b[2] - a[2])
        fname = (
            plan.formation_names[i].lower()
            if i < len(plan.formation_names)
            else plan.formation_names[-1].lower()
        )
        segs.append({
            "start_xyz": a,
            "end_xyz": b,
            "length": seg_len,
            "start_s": total_len,
            "formation": fname,
        })
        total_len += seg_len
    if total_len < 1e-6 or not segs:
        mission._log("[stage2] zero-length polyline; abort")
        return mark_streaming_abort(metrics, mission, "zero-length polyline")

    formations_planned_set = {n.lower() for n in plan.formation_names}
    formations_visited_set: set = set()

    # ---- Wall obstacle model for stage2 collision detection
    walls: List[Dict[str, float]] = []
    for w in stage.windows:
        walls.append({
            "y": w.center_global_xy[1],
            "gap_x_center": w.center_global_xy[0],
            "gap_half": w.gap_width / 2.0,
            "thickness": w.thickness,
            "z_min": w.distance_floor,
            "z_max": w.distance_floor + w.height,
        })

    def _wall_hit(px: float, py: float, pz: float) -> Tuple[bool, float]:
        """Return (any_wall_collision, min_clearance_to_any_wall_plane_m)."""
        min_clearance = float("inf")
        collided = False
        for wall in walls:
            dy = abs(py - wall["y"])
            if dy > (wall["thickness"] / 2.0 + 1.5):
                continue
            if dy < (wall["thickness"] / 2.0 + DRONE_RADIUS):
                in_gap_x = abs(px - wall["gap_x_center"]) < max(
                    0.0, wall["gap_half"] - DRONE_RADIUS
                )
                in_gap_z = (
                    wall["z_min"] + DRONE_RADIUS
                    < pz
                    < wall["z_max"] - DRONE_RADIUS
                )
                if not (in_gap_x and in_gap_z):
                    collided = True
            clearance = dy - wall["thickness"] / 2.0
            if clearance < min_clearance:
                min_clearance = clearance
        return collided, (min_clearance if min_clearance < float("inf") else -1.0)

    # ---- Telemetry buffers
    position_err_sqsum = 0.0
    position_err_samples = 0
    min_pair_dist_seen = float("inf")
    pair_collision_ticks = 0
    pair_ticks_physical = 0
    obstacle_collision_ticks = 0
    leader_path_len = 0.0
    last_leader_xy: Optional[Tuple[float, float]] = None
    formation_switch_count = 0
    tick_count = 0

    final_leader: Vec3 = entry
    final_followers_indexed: List[Vec3] = [(lp_x, lp_y, cruise_z) for _ in range(4)]

    def _interp_polyline(s_arc: float) -> Tuple[Vec3, str, int]:
        """Map arclength ``s_arc`` (m) to (leader_xyz, formation, seg_idx)."""
        if s_arc <= 0.0:
            sg0 = segs[0]
            return (tuple(sg0["start_xyz"]), str(sg0["formation"]), 0)  # type: ignore[return-value]
        for i, sg in enumerate(segs):
            start_s = float(sg["start_s"])  # type: ignore[arg-type]
            length = float(sg["length"])    # type: ignore[arg-type]
            if s_arc < start_s + length:
                seg_t = (s_arc - start_s) / max(1e-6, length)
                a = sg["start_xyz"]
                b = sg["end_xyz"]
                x = a[0] + (b[0] - a[0]) * seg_t  # type: ignore[index]
                y = a[1] + (b[1] - a[1]) * seg_t  # type: ignore[index]
                z = a[2] + (b[2] - a[2]) * seg_t  # type: ignore[index]
                return ((x, y, z), str(sg["formation"]), i)
        last = segs[-1]
        return (tuple(last["end_xyz"]), str(last["formation"]), len(segs) - 1)  # type: ignore[return-value]

    def _segment_yaw(seg_idx: int) -> float:
        """Yaw along segment seg_idx. Fall back to nearest valid segment."""
        for j in range(seg_idx, -1, -1):
            sg = segs[j]
            a = sg["start_xyz"]; b = sg["end_xyz"]  # type: ignore[assignment]
            dx = b[0] - a[0]; dy = b[1] - a[1]      # type: ignore[index]
            if math.hypot(dx, dy) >= 0.2:
                return math.atan2(dy, dx)
        return 0.0

    def _publish_swarm(leader_tgt: Vec3, slots_in_slot_order: List[Vec3],
                       yaw_cmd: float, twist: List[float]) -> None:
        if mission.dry_run:
            return
        mission._stream_position(mission.drones[0], leader_tgt, yaw_cmd, twist)
        for follower_idx in range(min(4, len(mission.drones) - 1)):
            slot_i = drone_to_slot[follower_idx]
            if 0 <= slot_i < len(slots_in_slot_order):
                ft = slots_in_slot_order[slot_i]
                mission._stream_position(mission.drones[1 + follower_idx], ft, yaw_cmd, twist)

    def _sample_telemetry(commanded_slots: List[Vec3]) -> None:
        nonlocal position_err_sqsum, position_err_samples
        nonlocal min_pair_dist_seen, pair_collision_ticks, pair_ticks_physical
        nonlocal obstacle_collision_ticks
        if mission.dry_run:
            return
        for follower_idx in range(min(4, len(mission.drones) - 1)):
            slot_i = drone_to_slot[follower_idx]
            if not (0 <= slot_i < len(commanded_slots)):
                continue
            tgt = commanded_slots[slot_i]
            try:
                p = mission.drones[1 + follower_idx].position
                err2 = (float(p[0]) - tgt[0]) ** 2 + (float(p[1]) - tgt[1]) ** 2
                position_err_sqsum += err2
                position_err_samples += 1
            except Exception:
                pass
        tick_min_pair = float("inf")
        any_wall_hit = False
        try:
            swarm_xyz: List[Tuple[float, float, float]] = []
            for d in mission.drones:
                p = d.position
                swarm_xyz.append((float(p[0]), float(p[1]), float(p[2])))
            for i in range(len(swarm_xyz)):
                for j in range(i + 1, len(swarm_xyz)):
                    d_pair = math.hypot(
                        swarm_xyz[i][0] - swarm_xyz[j][0],
                        swarm_xyz[i][1] - swarm_xyz[j][1],
                    )
                    if d_pair < tick_min_pair:
                        tick_min_pair = d_pair
                hit, _ = _wall_hit(*swarm_xyz[i])
                if hit:
                    any_wall_hit = True
        except Exception:
            pass
        if tick_min_pair < min_pair_dist_seen:
            min_pair_dist_seen = tick_min_pair
        if tick_min_pair < PAIR_SAFETY_M:
            pair_collision_ticks += 1
        if tick_min_pair < PAIR_PHYSICAL_M:
            pair_ticks_physical += 1
        if any_wall_hit:
            obstacle_collision_ticks += 1

    
    # ---- Phase 0: prime pipeline at current drone XY, cruise z.
    if not mission.dry_run:
        for drone in mission.drones:
            try:
                cur = drone.position
                px = float(cur[0]); py = float(cur[1])
            except Exception:
                px, py = lp_x, lp_y
            mission._stream_position(drone, (px, py, cruise_z), slew_yaw, twist_slew)
        time.sleep(0.5)

    # ---- Phase 1: Rally in place (leader holds, followers -> columnn).
    if not mission.dry_run and rally_duration > 0.0:
        rally_slots_xyz: List[Vec3] = [(p[0], p[1], cruise_z) for p in rally_slots_global_xy]
        t0 = time.time()
        while time.time() - t0 < rally_duration:
            _publish_swarm((lp_x, lp_y, cruise_z), rally_slots_xyz, slew_yaw, twist_rally)
            tick_count += 1
            final_leader = (lp_x, lp_y, cruise_z)
            final_followers_indexed = rally_slots_xyz
            time.sleep(dt)
        mission._log(f"[stage2] Rally complete, formed {initial_form.upper()} at leader position")

    # ---- Phase 2: Ferry from leader's current XY to entry waypoint.
    slew_dist = math.hypot(entry_x - lp_x, entry_y - lp_y) + abs(entry_z - cruise_z)
    if slew_dist > 0.05:
        T_slew = max(1.5, slew_dist / slew_speed)
        t0 = time.time()
        while True:
            e = time.time() - t0
            if e >= T_slew:
                break
            frac = e / T_slew
            lx = lp_x + (entry_x - lp_x) * frac
            ly = lp_y + (entry_y - lp_y) * frac
            lz = cruise_z + (entry_z - cruise_z) * frac
            leader_target: Vec3 = (lx, ly, lz)
            offs = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
            slot_targets: List[Vec3] = []
            for off in offs:
                dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
                slot_targets.append((lx + dxg, ly + dyg, lz + off[2]))
            _publish_swarm(leader_target, slot_targets, slew_yaw, twist_slew)
            tick_count += 1
            final_leader = leader_target
            final_followers_indexed = slot_targets
            time.sleep(dt)
        mission._log(f"[stage2] Ferry complete, leader at entry ({entry_x:.2f}, {entry_y:.2f}, {entry_z:.2f})")

    # ---- Phase 3: Streaming cruise along the planned polyline.
    previous_formation = initial_form
    formations_visited_set.add(initial_form)
    s_arc = 0.0
    last_log = 0.0
    t_start = time.time()
    T_max = (total_len / target_speed) * 2.0 + 20.0
    while True:
        t_rel = time.time() - t_start
        if t_rel > T_max:
            mission._log(f"[stage2] cruise timed out at t={t_rel:.1f}s, s={s_arc:.2f}/{total_len:.2f}")
            break
        s_arc += target_speed * dt
        leader_target, formation, seg_idx = _interp_polyline(s_arc)
        lx, ly, lz = leader_target
        yaw = _segment_yaw(seg_idx)

        sg = segs[seg_idx]
        prev_formation_name = (
            str(segs[seg_idx - 1]["formation"]) if seg_idx - 1 >= 0 else initial_form
        )
        seg_t_arc = s_arc - float(sg["start_s"])  # type: ignore[arg-type]
        seg_time_equiv = seg_t_arc / max(1e-6, target_speed)
        if formation != prev_formation_name and seg_time_equiv < transition_s:
            alpha = max(0.0, min(1.0, seg_time_equiv / transition_s))
        else:
            alpha = 1.0

        cur_offs = _get_offsets(formation, spacing=default_spacing, z=0.0)
        if formation != prev_formation_name and alpha < 1.0:
            prev_offs = _get_offsets(prev_formation_name, spacing=default_spacing, z=0.0)
            offs_local = [
                (
                    po[0] + (co[0] - po[0]) * alpha,
                    po[1] + (co[1] - po[1]) * alpha,
                    po[2] + (co[2] - po[2]) * alpha,
                )
                for po, co in zip(prev_offs, cur_offs)
            ]
        else:
            offs_local = list(cur_offs)

        slot_targets = []
        for off in offs_local:
            dxg, dyg = _rotate_xy(off[0], off[1], yaw)
            slot_targets.append((lx + dxg, ly + dyg, lz + off[2]))

        _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
        _sample_telemetry(slot_targets)

        if not mission.dry_run:
            try:
                p = mission.drones[0].position
                axy = (float(p[0]), float(p[1]))
                if last_leader_xy is not None:
                    leader_path_len += math.hypot(axy[0] - last_leader_xy[0], axy[1] - last_leader_xy[1])
                last_leader_xy = axy
            except Exception:
                pass

        tick_count += 1
        metrics.completed_waypoints = tick_count
        final_leader = leader_target
        final_followers_indexed = slot_targets

        if formation != previous_formation:
            mission._log(f"[stage2] FORMATION -> {formation.upper()}")
            formation_switch_count += 1
        previous_formation = formation
        formations_visited_set.add(formation)

        if time.time() - last_log >= 1.0:
            last_log = time.time()
            
        if s_arc >= total_len:
            break
        time.sleep(dt)

    # ---- Phase 4: hover at final pose.
    if not mission.dry_run:
        hover_yaw = 0.0
        hover_twist = [0.2, 0.2, 0.2]
        t0 = time.time()
        while time.time() - t0 < hover_duration:
            _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
            time.sleep(dt)

    # ---- Finalise metrics
    metrics.planned_waypoints = len(plan.waypoints)
    metrics.end_time = time.time()

    if position_err_samples > 0:
        metrics.formation_rmse_mean = math.sqrt(position_err_sqsum / position_err_samples)
    if min_pair_dist_seen < float("inf"):
        metrics.min_pair_distance_m = min_pair_dist_seen
    metrics.completion_ratio = max(0.0, min(1.0, s_arc / total_len))
    metrics.formation_switches = formation_switch_count

    # Axis 2: Reconfigurability
    metrics.formations_planned = len(formations_planned_set)
    metrics.formations_visited = len(formations_visited_set & formations_planned_set)

    # Axis 4: Collision avoidance
    metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m strict
    metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m physical
    metrics.num_obstacle_collision_ticks = obstacle_collision_ticks

    # Axis 5: Efficiency
    metrics.leader_path_length_m = leader_path_len
    metrics.ideal_path_length_m = total_len

    # Time-taken breakdown: allocate per-segment duration (seg_len / target_speed)
    # to each formation name along the planned polyline.
    metrics.cruise_time_s = total_len / target_speed if target_speed > 1e-6 else None
    tpf: Dict[str, float] = {}
    for sg in segs:
        name = str(sg["formation"])
        seg_dur = float(sg["length"]) / target_speed if target_speed > 1e-6 else 0.0  # type: ignore[arg-type]
        tpf[name] = tpf.get(name, 0.0) + seg_dur
    metrics.time_per_formation = tpf

    # Stage2-specific fairness metric: how many planned windows did the
    # leader actually pass through? A "passed" window is one whose
    # ``seg_idx`` segment was fully traversed, i.e. the leader advanced
    # s_arc past the end of that window-crossing segment.
    if mission.scenario.stage2 is not None:
        metrics.windows_planned = len(mission.scenario.stage2.windows)
        # Geometric counting: for each planned window, find the segment
        # whose end waypoint sits on that window's y-centre (the ``inside``
        # waypoint produced by the planner), and count the window as
        # passed iff the leader's arclength ``s_arc`` reached the end of
        # that segment. This gives one count per unique window rather
        # than one per ``columnn`` sub-segment (pre/inside/post/...).
        passed = 0
        for w in mission.scenario.stage2.windows:
            wy = float(w.center_global_xy[1])
            for sg in segs:
                end_xyz = sg["end_xyz"]
                if abs(float(end_xyz[1]) - wy) < 0.05:
                    end_s = float(sg["start_s"]) + float(sg["length"])  # type: ignore[arg-type]
                    if s_arc >= end_s - 0.05:
                        passed += 1
                    break
        metrics.windows_passed = passed

    # Axis 1: Success (dual threshold)
    cr = metrics.completion_ratio if metrics.completion_ratio is not None else 0.0
    recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
    no_obstacle = (obstacle_collision_ticks == 0)
    metrics.success_strict = bool(
        cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_collision_ticks == 0
    )
    metrics.success_physical = bool(
        cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_ticks_physical == 0
    )
    metrics.success = metrics.success_strict  # back-compat
    metrics.success_criterion = (
        "completion_ratio>=0.95 AND reconfigurability==1.0 AND "
        "wall_hit_ticks==0 AND pair_collision_ticks==0 "
        "(@0.30m strict / @0.15m physical)"
    )

    mission.metrics[stage_name] = metrics
    return metrics
