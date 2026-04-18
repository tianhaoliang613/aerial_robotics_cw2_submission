"""Stage 1 streaming runner."""
from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, Tuple

from centralised.metrics_export import StageMetrics, mark_streaming_abort
from centralised.utils import greedy_slot_assignment as _greedy_slot_assignment

Vec3 = Tuple[float, float, float]

def run_stage1_streaming_impl(mission: Any) -> StageMetrics:
    """Stage1: 10 Hz streaming circle cruise; greedy rally slots; metrics on StageMetrics."""
    stage_name = "stage1"
    stage = mission.scenario.stage1
    metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
    metrics.start_time = time.time()
    if stage is None:
        metrics.end_time = time.time()
        mission.metrics[stage_name] = metrics
        return metrics

    if not mission.dry_run and not mission._ensure_motion_ref_loaded():
        mission._log("[stage1] motion_ref load failed; abort")
        return mark_streaming_abort(metrics, mission, "motion_ref unavailable")

    cx, cy = stage.stage_center
    radius = stage.diameter / 2.0
    z = mission.planner.cruise_height

    formations = mission.planner._stage1_formations(stage.formations)
    num_forms = max(1, len(formations))
    rate_hz = 10.0
    dt = 1.0 / rate_hz
    twist_slew = [0.6, 0.6, 0.3]
    twist_cruise = [0.8, 0.8, 0.4]
    twist_rally = [0.5, 0.5, 0.3]
    omega = 0.20
    formation_period = math.pi / omega
    num_loops = max(1.5, num_forms * 0.5)
    total_angle = 2.0 * math.pi * num_loops
    T_cruise = total_angle / omega
    transition_s = min(4.0, formation_period * 0.3)

    # ---- Pick theta0 = angle of leader's current position w.r.t. ring
    try:
        if not mission.dry_run:
            lp = mission.drones[0].position
            lp_x, lp_y = float(lp[0]), float(lp[1])
        else:
            lp_x, lp_y = cx + radius, cy
    except Exception:
        lp_x, lp_y = cx + radius, cy
    dx0 = lp_x - cx
    dy0 = lp_y - cy
    if math.hypot(dx0, dy0) < 1e-3:
        theta0 = 0.0
    else:
        theta0 = math.atan2(dy0, dx0)

    entry_x = cx + radius * math.cos(theta0)
    entry_y = cy + radius * math.sin(theta0)
    slew_dist = math.hypot(entry_x - lp_x, entry_y - lp_y)
    # Cap slew_speed at twist_slew[0] so the drones can actually keep
    # up with the commanded target (critical fix: prev revision used
    # 1.5 m/s while twist was 0.8 m/s, so drones fell 50% behind).
    slew_speed = min(0.6, twist_slew[0])
    T_slew = max(1.0, slew_dist / slew_speed) if slew_dist > 0.05 else 0.0
    slew_yaw = math.atan2(entry_y - lp_y, entry_x - lp_x) if slew_dist > 1e-3 else (theta0 + math.pi / 2.0)

    # ---- Greedy slot-assignment for the initial ``line`` formation
    # The FollowerController returns 4 offsets in a fixed order, rotated
    # by the leader's heading. We ALSO need to decide which physical
    # drone (drone1..drone4) flies which slot. Namespace-ordered
    # assignment causes crossing paths -- see docstring. Compute
    # nearest-neighbour assignment ONCE here, then freeze it for the
    # whole stage1 run so slots don't swap mid-flight.
    default_spacing = mission.controller.default_spacing
    # Line offsets in drone-local frame (x along nose, y left).
    # orbit's leader-crossing fix is now baked into
    # ``centralised/formations.py`` (diagonal slot layout), so stage1
    # no longer needs a local override -- any stage that imports
    # ``get_formation_offsets`` will get the collision-safe orbit.
    from centralised.formations import get_formation_offsets as _get_offsets
    from centralised.utils import rotate_xy as _rotate_xy
    line_offsets_local = _get_offsets("line", spacing=default_spacing, z=0.0)
    line_slots_global: List[Tuple[float, float]] = []
    for off in line_offsets_local:
        dx, dy = _rotate_xy(off[0], off[1], slew_yaw)
        line_slots_global.append((lp_x + dx, lp_y + dy))
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
    slot_to_follower = _greedy_slot_assignment(follower_positions_now, line_slots_global)
    # follower_index (0..3) -> mission.drones[1 + follower_index]
    # slot_to_drone_idx[slot] = follower_idx (0..3).
    # Invert to drone_to_slot[follower_idx] = slot.
    drone_to_slot: List[int] = [0] * 4
    for slot_i, drone_idx in enumerate(slot_to_follower):
        if 0 <= drone_idx < 4:
            drone_to_slot[drone_idx] = slot_i

    
    tick_count = 0
    final_leader: Vec3 = (entry_x, entry_y, z)
    final_followers_indexed: List[Vec3] = [(lp_x, lp_y, z) for _ in range(4)]

    # ---- Per-tick telemetry buffers for post-run metrics.
    position_err_sqsum = 0.0       # sum of squared (actual - target) xy distances
    position_err_samples = 0
    min_pair_dist_seen = float("inf")
    # Official collision-avoidance metric: number of ticks during which
    # ANY pair of drones was closer than PAIR_SAFETY_M. 0 => no close
    # calls at any point during the cruise.
    PAIR_SAFETY_M = 0.30
    PAIR_PHYSICAL_M = 0.15  # physical body + propeller radius sum
    pair_collision_ticks = 0
    pair_ticks_physical = 0
    leader_path_len = 0.0
    last_leader_xy: Optional[Tuple[float, float]] = None
    cruise_theta_start: Optional[float] = None
    cruise_theta_last: Optional[float] = None
    cruise_theta_travel = 0.0
    formation_switch_count = 0
    # Official reconfigurability metric: set of formation names the
    # cruise loop actually commanded (and therefore visibly displayed).
    formations_visited_set: set = set()

    def _publish_swarm(leader_tgt: Vec3, slots_in_slot_order: List[Vec3],
                       yaw_cmd: float, twist: List[float]) -> None:
        """Send leader + each follower's assigned slot target.

        ``slots_in_slot_order[slot_i]`` is the WORLD target of slot i.
        ``mission.drones[1 + follower_idx]`` gets the slot that follower
        is assigned to via ``drone_to_slot``.
        """
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
        if mission.dry_run:
            return
        # Formation RMSE: per-follower |actual - commanded slot|
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
        # Minimum pairwise XY distance across the whole swarm (this tick).
        tick_min_pair = float("inf")
        try:
            swarm_xy: List[Tuple[float, float]] = []
            for d in mission.drones:
                p = d.position
                swarm_xy.append((float(p[0]), float(p[1])))
            for i in range(len(swarm_xy)):
                for j in range(i + 1, len(swarm_xy)):
                    d2 = math.hypot(swarm_xy[i][0] - swarm_xy[j][0], swarm_xy[i][1] - swarm_xy[j][1])
                    if d2 < tick_min_pair:
                        tick_min_pair = d2
        except Exception:
            pass
        if tick_min_pair < min_pair_dist_seen:
            min_pair_dist_seen = tick_min_pair
        if tick_min_pair < PAIR_SAFETY_M:
            pair_collision_ticks += 1
        if tick_min_pair < PAIR_PHYSICAL_M:
            pair_ticks_physical += 1

    # ---- Phase 0: prime pipeline at current drone XY, cruise z.
    if not mission.dry_run:
        for drone in mission.drones:
            try:
                cur = drone.position
                px = float(cur[0]); py = float(cur[1])
            except Exception:
                px, py = lp_x, lp_y
            mission._stream_position(drone, (px, py, z), slew_yaw, twist_slew)
        time.sleep(0.5)

    # ---- Phase 1: Rally in place (leader stationary, followers go to line slots)
    T_rally = 3.0
    if not mission.dry_run and T_rally > 0.0:
        rally_slots_xyz: List[Vec3] = [(p[0], p[1], z) for p in line_slots_global]
        t0 = time.time()
        while True:
            e = time.time() - t0
            if e >= T_rally:
                break
            _publish_swarm((lp_x, lp_y, z), rally_slots_xyz, slew_yaw, twist_rally)
            tick_count += 1
            final_leader = (lp_x, lp_y, z)
            final_followers_indexed = rally_slots_xyz
            time.sleep(dt)
        mission._log("[stage1] Rally complete, leader hold at spawn, followers in LINE")

    # ---- Phase 2: ferry to ring entry
    if T_slew > 0.0:
        t0 = time.time()
        while True:
            e = time.time() - t0
            if e >= T_slew:
                break
            frac = e / T_slew
            lx = lp_x + (entry_x - lp_x) * frac
            ly = lp_y + (entry_y - lp_y) * frac
            leader_target: Vec3 = (lx, ly, z)
            slot_targets: List[Vec3] = []
            for off in line_offsets_local:
                dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
                slot_targets.append((lx + dxg, ly + dyg, z + off[2]))
            _publish_swarm(leader_target, slot_targets, slew_yaw, twist_slew)
            tick_count += 1
            final_leader = leader_target
            final_followers_indexed = slot_targets
            time.sleep(dt)
        mission._log(f"[stage1] Ferry complete, leader at ring entry ({entry_x:.2f}, {entry_y:.2f})")

    # ---- Phase 3: cruise around ring with scheduled formations
    previous_formation = formations[0]
    last_log = 0.0
    t_start = time.time()
    while True:
        t_rel = time.time() - t_start
        if t_rel >= T_cruise:
            break

        theta = theta0 + omega * t_rel
        lx = cx + radius * math.cos(theta)
        ly = cy + radius * math.sin(theta)
        yaw = theta + math.pi / 2.0
        leader_target = (lx, ly, z)

        # Scheduled formation + linear blend at segment boundary.
        seg_float = t_rel / formation_period
        seg_idx = int(seg_float) % num_forms
        seg_t = t_rel - seg_idx * formation_period
        formation = formations[seg_idx]
        prev_name = formations[(seg_idx - 1) % num_forms] if seg_idx > 0 else formations[0]
        if formation != prev_name and seg_t < transition_s:
            alpha = max(0.0, min(1.0, seg_t / transition_s))
        else:
            alpha = 1.0

        # Compute slot targets directly (so we publish per drone_to_slot).
        cur_offs_local = _get_offsets(formation, spacing=default_spacing, z=0.0)
        if formation != prev_name and alpha < 1.0:
            prev_offs_local = _get_offsets(prev_name, spacing=default_spacing, z=0.0)
            blended_local: List[Tuple[float, float, float]] = []
            for po, co in zip(prev_offs_local, cur_offs_local):
                blended_local.append(
                    (po[0] + (co[0] - po[0]) * alpha,
                     po[1] + (co[1] - po[1]) * alpha,
                     po[2] + (co[2] - po[2]) * alpha)
                )
            offs_local = blended_local
        else:
            offs_local = list(cur_offs_local)
        slot_targets = []
        for off in offs_local:
            dxg, dyg = _rotate_xy(off[0], off[1], yaw)
            slot_targets.append((lx + dxg, ly + dyg, z + off[2]))

        _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
        _sample_telemetry(slot_targets)

        # Track leader actual path length and angular travel.
        if not mission.dry_run:
            try:
                p = mission.drones[0].position
                axy = (float(p[0]), float(p[1]))
                if last_leader_xy is not None:
                    leader_path_len += math.hypot(axy[0] - last_leader_xy[0], axy[1] - last_leader_xy[1])
                last_leader_xy = axy
                cur_theta = math.atan2(axy[1] - cy, axy[0] - cx)
                if cruise_theta_start is None:
                    cruise_theta_start = cur_theta
                    cruise_theta_last = cur_theta
                else:
                    d_theta = cur_theta - (cruise_theta_last or cur_theta)
                    if d_theta > math.pi:
                        d_theta -= 2 * math.pi
                    elif d_theta < -math.pi:
                        d_theta += 2 * math.pi
                    cruise_theta_travel += d_theta
                    cruise_theta_last = cur_theta
            except Exception:
                pass

        tick_count += 1
        metrics.completed_waypoints = tick_count
        final_leader = leader_target
        final_followers_indexed = slot_targets

        if formation != previous_formation:
            mission._log(f"[stage1] FORMATION -> {formation.upper()}")
            formation_switch_count += 1
        previous_formation = formation
        formations_visited_set.add(formation)

        if time.time() - last_log >= 1.0:
            last_log = time.time()
            
        time.sleep(dt)

    # ---- Phase 4: hover at final pose
    if not mission.dry_run:
        hover_yaw = 0.0
        hover_twist = [0.2, 0.2, 0.2]
        t0 = time.time()
        while time.time() - t0 < 1.0:
            _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
            time.sleep(dt)

    # ---- Finalise metrics
    metrics.planned_waypoints = max(tick_count, metrics.planned_waypoints)
    metrics.end_time = time.time()

    # Centralised-specific analytics (detail, not pass/fail).
    if position_err_samples > 0:
        metrics.formation_rmse_mean = math.sqrt(position_err_sqsum / position_err_samples)
    if min_pair_dist_seen < float("inf"):
        metrics.min_pair_distance_m = min_pair_dist_seen
    planned_angular = abs(omega * T_cruise)
    if planned_angular > 1e-6:
        metrics.completion_ratio = max(0.0, min(1.0, abs(cruise_theta_travel) / planned_angular))
    metrics.formation_switches = formation_switch_count

    # === OFFICIAL metric axis 2: Reconfigurability =========================
    metrics.formations_planned = num_forms
    metrics.formations_visited = len(formations_visited_set)

    # === OFFICIAL metric axis 4: Collision avoidance =======================
    metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m threshold
    metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m threshold
    # Stage1 has no static/dynamic obstacles -> leave obstacle fields as None.

    # === OFFICIAL metric axis 5: Efficiency in movement ====================
    metrics.leader_path_length_m = leader_path_len
    # Ideal arc length along the commanded ring over the planned cruise.
    metrics.ideal_path_length_m = 2.0 * math.pi * radius * num_loops

    # === Time-taken breakdown ==============================================
    # 1 lap = 2pi / omega seconds (e.g. omega=0.20 -> 31.42 s).
    metrics.time_per_lap_s = (2.0 * math.pi / omega) if omega > 1e-6 else None
    metrics.cruise_time_s = T_cruise
    metrics.laps_completed = (
        abs(cruise_theta_travel) / (2.0 * math.pi)
        if cruise_theta_travel is not None
        else None
    )
    # Per-formation display duration: in stage1 every formation holds for
    # exactly ``formation_period`` seconds, repeated (num_loops * 2 / num_forms)
    # times over the cruise. Compute shown duration per unique formation name.
    num_half_laps = num_loops * 2.0
    count_per_formation: Dict[str, int] = {}
    for i in range(int(math.floor(num_half_laps))):
        name = formations[i % num_forms]
        count_per_formation[name] = count_per_formation.get(name, 0) + 1
    # Handle fractional tail (e.g. 0.5 extra half-lap).
    frac = num_half_laps - math.floor(num_half_laps)
    if frac > 1e-6:
        name = formations[int(math.floor(num_half_laps)) % num_forms]
        metrics.time_per_formation = {
            n: count_per_formation.get(n, 0) * formation_period
            for n in formations
        }
        metrics.time_per_formation[name] = (
            metrics.time_per_formation.get(name, 0.0) + frac * formation_period
        )
    else:
        metrics.time_per_formation = {
            n: count_per_formation.get(n, 0) * formation_period
            for n in formations
        }

    # === OFFICIAL metric axis 1: Success rate (dual threshold) =============
    # Stage 1 success criteria (shared by strict & physical variants):
    #   (a) completion_ratio >= 0.95  (leader actually circled the ring)
    #   (b) reconfigurability == 1.0  (every planned formation shown)
    #   (c) pair-collision tick count == 0
    # Strict:   (c) uses 0.30 m near-miss threshold  (conservative)
    # Physical: (c) uses 0.15 m body-contact threshold (lenient, matches
    #           physical crazyflie radius)
    cr = metrics.completion_ratio if metrics.completion_ratio is not None else 0.0
    recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
    metrics.success_strict = bool(
        cr >= 0.95 and recfg >= 0.999 and pair_collision_ticks == 0
    )
    metrics.success_physical = bool(
        cr >= 0.95 and recfg >= 0.999 and pair_ticks_physical == 0
    )
    # Back-compat: ``success`` mirrors the strict variant.
    metrics.success = metrics.success_strict
    metrics.success_criterion = (
        "completion_ratio>=0.95 AND reconfigurability==1.0 AND "
        "pair_collision_ticks==0 (@0.30m strict / @0.15m physical)"
    )

    mission.metrics[stage_name] = metrics
    return metrics

