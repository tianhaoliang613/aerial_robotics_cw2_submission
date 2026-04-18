"""Stage 3 streaming runner."""
from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Tuple

from centralised.metrics_export import StageMetrics, mark_streaming_abort
from centralised.streaming_telemetry import (
    min_pairwise_xy_distance,
    pair_violation_flags,
)
from centralised.utils import greedy_slot_assignment as _greedy_slot_assignment

Vec3 = Tuple[float, float, float]


def run_stage3_streaming_impl(mission: Any) -> StageMetrics:
    """Stage3: 10 Hz forest polyline cruise; cylindrical tree hits; detour_ratio."""
    stage_name = "stage3"
    stage = mission.scenario.stage3
    metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
    metrics.start_time = time.time()
    if stage is None:
        metrics.end_time = time.time()
        mission.metrics[stage_name] = metrics
        return metrics

    if not mission.dry_run and not mission._ensure_motion_ref_loaded():
        mission._log("[stage3] motion_ref load failed; abort")
        return mark_streaming_abort(metrics, mission, "motion_ref unavailable")

    plan = mission.planner.plan_stage3()
    if not plan.waypoints:
        mission._log("[stage3] empty plan; abort")
        return mark_streaming_abort(metrics, mission, "empty plan")

    cruise_z = mission.planner.cruise_height
    default_spacing = mission.controller.default_spacing

    rate_hz = 10.0
    dt = 1.0 / rate_hz
    target_speed = 0.35   # leader linear speed along polyline (m/s)
    slew_speed = 0.40     # m/s during ferry
    rally_duration = 2.5
    hover_duration = 1.0
    twist_slew = [0.6, 0.6, 0.5]
    twist_cruise = [0.6, 0.6, 0.5]
    twist_rally = [0.4, 0.4, 0.3]
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

    # ---- Greedy slot assignment for initial rally (stage3 plan is all columnn)
    initial_form = "columnn"
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
        segs.append({
            "start_xyz": a,
            "end_xyz": b,
            "length": seg_len,
            "start_s": total_len,
            "formation": "columnn",
        })
        total_len += seg_len
    if total_len < 1e-6 or not segs:
        mission._log("[stage3] zero-length polyline; abort")
        return mark_streaming_abort(metrics, mission, "zero-length polyline")

    formations_planned_set = {"columnn"}
    formations_visited_set: set = set()

    # ---- Tree (cylindrical) obstacle model for stage3 collision detection.
    # A drone is considered to have HIT a tree if its XY distance to the
    # tree centre is less than (tree_radius + drone_radius) and it is
    # below the tree's height. At cruise_z = 1.2 m the drones are always
    # below the 5 m tree height so we only need the XY check.
    tree_radius = stage.obstacle_diameter / 2.0
    trees: List[Tuple[float, float]] = list(stage.obstacles_global)

    def _tree_hit(px: float, py: float, pz: float) -> bool:
        """True iff this drone XY is inside any tree's body footprint."""
        if pz > stage.obstacle_height + 0.1:
            return False
        for (tx, ty) in trees:
            if math.hypot(px - tx, py - ty) < (tree_radius + DRONE_RADIUS):
                return True
        return False

    # ---- Telemetry buffers
    min_pair_dist_seen = float("inf")
    pair_collision_ticks = 0
    pair_ticks_physical = 0
    obstacle_collision_ticks = 0
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

    def _sample_telemetry() -> None:
        nonlocal min_pair_dist_seen, pair_collision_ticks, pair_ticks_physical
        nonlocal obstacle_collision_ticks
        if mission.dry_run:
            return
        tick_min_pair = float("inf")
        any_tree_hit = False
        try:
            swarm_xyz: List[Tuple[float, float, float]] = []
            for d in mission.drones:
                p = d.position
                swarm_xyz.append((float(p[0]), float(p[1]), float(p[2])))
            md = min_pairwise_xy_distance([(x, y) for x, y, _ in swarm_xyz])
            if md is not None:
                tick_min_pair = md
            any_tree_hit = any(_tree_hit(x, y, z) for x, y, z in swarm_xyz)
        except Exception:
            pass
        if tick_min_pair < min_pair_dist_seen:
            min_pair_dist_seen = tick_min_pair
        strict, physical = pair_violation_flags(tick_min_pair)
        if strict:
            pair_collision_ticks += 1
        if physical:
            pair_ticks_physical += 1
        if any_tree_hit:
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
    # The initial formation is ``columnn`` aligned with the ferry
    # direction, so followers line up behind the leader along the
    # eventual direction of travel -- no wide lateral spread that
    # could clip trees at the forest entry.
    if not mission.dry_run and rally_duration > 0.0:
        rally_slots_xyz: List[Vec3] = [(p[0], p[1], cruise_z) for p in rally_slots_global_xy]
        t0 = time.time()
        while time.time() - t0 < rally_duration:
            _publish_swarm((lp_x, lp_y, cruise_z), rally_slots_xyz, slew_yaw, twist_rally)
            _sample_telemetry()
            tick_count += 1
            final_leader = (lp_x, lp_y, cruise_z)
            final_followers_indexed = rally_slots_xyz
            time.sleep(dt)

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
            _sample_telemetry()
            tick_count += 1
            final_leader = leader_target
            final_followers_indexed = slot_targets
            time.sleep(dt)

    # ---- Phase 3: Streaming cruise along the planned polyline (columnn trail).
    formations_visited_set.add(initial_form)
    s_arc = 0.0
    t_start = time.time()
    T_max = (total_len / target_speed) * 2.0 + 20.0
    spacing_col = min(default_spacing, 0.5)  # matches column_n()
    while True:
        t_rel = time.time() - t_start
        if t_rel > T_max:
            mission._log(f"[stage3] cruise timed out at t={t_rel:.1f}s, s={s_arc:.2f}/{total_len:.2f}")
            break
        s_arc += target_speed * dt
        leader_target, formation, seg_idx = _interp_polyline(s_arc)
        lx, ly, lz = leader_target
        yaw = _segment_yaw(seg_idx)

        # Trail-following for columnn: follower k targets the leader's
        # polyline at arclength (s_arc - (k+1)*spacing). A* guarantees
        # the leader path clears every tree, so trailing followers do
        # too.
        slot_targets: List[Vec3] = []
        for k in range(4):
            s_k = s_arc - (k + 1) * spacing_col
            if s_k <= 0.0:
                dxg, dyg = _rotate_xy(-(k + 1) * spacing_col, 0.0, yaw)
                slot_targets.append((lx + dxg, ly + dyg, lz))
            else:
                tgt, _, _ = _interp_polyline(s_k)
                slot_targets.append(tgt)

        _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
        _sample_telemetry()

        tick_count += 1
        metrics.completed_waypoints = tick_count
        final_leader = leader_target
        final_followers_indexed = slot_targets

        formations_visited_set.add(formation)

        if s_arc >= total_len:
            break
        time.sleep(dt)

    # ---- Phase 4: brief hover at final pose so the columnn is visible.
    hover_yaw = _segment_yaw(len(segs) - 1)
    hover_twist = [0.2, 0.2, 0.2]
    if not mission.dry_run:
        t0 = time.time()
        while time.time() - t0 < hover_duration:
            _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
            time.sleep(dt)

    # ---- Finalise metrics
    metrics.planned_waypoints = len(plan.waypoints)
    metrics.end_time = time.time()

    if min_pair_dist_seen < float("inf"):
        metrics.min_pair_distance_m = min_pair_dist_seen
    metrics.completion_ratio = max(0.0, min(1.0, s_arc / total_len))

    # Axis 2: Reconfigurability
    metrics.formations_planned = len(formations_planned_set)
    metrics.formations_visited = len(formations_visited_set & formations_planned_set)

    # Axis 4: Collision avoidance
    metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m strict
    metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m physical
    metrics.num_obstacle_collision_ticks = obstacle_collision_ticks

    # Axis 5: Efficiency (planned polyline length only; no measured leader path)
    metrics.ideal_path_length_m = total_len

    # Time-taken breakdown
    metrics.cruise_time_s = total_len / target_speed if target_speed > 1e-6 else None
    metrics.time_per_formation = (
        {"columnn": total_len / target_speed} if target_speed > 1e-6 else {}
    )

    # detour_ratio: planned polyline length / straight-line start→end
    sx, sy = stage.start_point_global
    ex, ey = stage.end_point_global
    straight = math.hypot(ex - sx, ey - sy)
    if straight > 1e-6:
        metrics.detour_ratio = total_len / straight

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
        "tree_hit_ticks==0 AND pair_collision_ticks==0 "
        "(@0.30m strict / @0.15m physical)"
    )

    mission.metrics[stage_name] = metrics
    return metrics
