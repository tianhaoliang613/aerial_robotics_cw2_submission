"""Stage 4 streaming reactive cruise."""
from __future__ import annotations

import math
import time
from typing import Any, List, Optional, Tuple

try:
    from as2_msgs.msg import YawMode
except ImportError:  # dry-run without ROS2
    class _YawMode:
        PATH_FACING = 0
    YawMode = _YawMode  # type: ignore[misc,assignment]

from centralised.metrics_export import StageMetrics

Vec3 = Tuple[float, float, float]


def run_stage4_impl(mission: Any) -> StageMetrics:
    from centralised.obstacle_avoidance import avoid_dynamic_obstacles
    stage_name = "stage4"
    stage = mission.scenario.stage4
    metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
    metrics.start_time = time.time()
    if stage is None:
        metrics.end_time = time.time()
        mission.metrics[stage_name] = metrics
        return metrics

    
    z = 1.8
    start_x, start_y = stage.start_point_global
    end_x, end_y = stage.end_point_global
    total_dist = math.hypot(end_x - start_x, end_y - start_y)
    cruise_speed = 0.8
    nominal_yaw = math.atan2(end_y - start_y, end_x - start_x)
    leader_radius = 0.3
    safe_dist = (stage.obstacle_diameter / 2.0) + leader_radius + 2.0
    compress_trigger = 3.5
    forward_lookahead_m = 5.0
    swarm_spacing = 1.0
    rally_leader: Vec3 = (start_x, start_y, z)
    rally_followers = mission.controller.compute_targets(
        leader_position=rally_leader,
        leader_yaw=nominal_yaw,
        formation_name="diamond",
        previous_formation_name="diamond",
        transition_alpha=1.0,
        static_obstacles=None,
    )

    # ---------------- Phase 1: Rally at start (blocking go_to) ----------------
    if not mission.dry_run:
        rally_ok = mission._command_swarm_step(
            leader_target=rally_leader,
            follower_targets=rally_followers,
            speed=cruise_speed,
            yaw_mode=YawMode.PATH_FACING,
            yaw_angle=None,
        )
        if not rally_ok:
            mission._log("[stage4] rally to start_point failed; aborting")
            metrics.end_time = time.time()
            mission.metrics[stage_name] = metrics
            return metrics

    # ---------------- Phase 2: Obstacle warmup ----------------
    if mission.obstacle_monitor is not None and not mission.dry_run:
        t0 = time.time()
        while time.time() - t0 < 4.0 and mission.obstacle_monitor.count() < stage.num_obstacles:
            time.sleep(0.15)

    if not mission.dry_run:
        if not mission._ensure_motion_ref_loaded():
            mission._log("[stage4] motion_ref_handler load failed; aborting reactive cruise")
            metrics.end_time = time.time()
            mission.metrics[stage_name] = metrics
            return metrics
        for drone in mission.drones:
            try:
                cur = drone.position
                cx = float(cur[0]); cy = float(cur[1]); cz = float(cur[2])
            except Exception:
                cx, cy, cz = 0.0, 0.0, z
            cz_cmd = z
            mission._stream_position(drone, (cx, cy, cz_cmd), nominal_yaw, [1.2, 1.2, 0.6])
        time.sleep(0.8)

    rate_hz = 10.0
    dt_sleep = 1.0 / rate_hz
    T_max = max(25.0, (total_dist / 0.6) * 2.0 + 10.0)
    twist_limit = [0.6, 0.6, 0.6]
    lookahead_m = 2.5
    previous_formation = "diamond"
    t_start = time.time()
    tick_count = 0
    final_reached = False
    last_leader_target: Optional[Vec3] = None
    shift_dx_f = 0.0
    shift_dy_f = 0.0
    MAX_SHIFT = 3.0
    SHIFT_ALPHA = 0.5
    AVOID_Z_MIN = 0.4
    z_min_safe = 1.3
    low_z_ticks = 0
    PAUSE_TICKS = 2
    last_avoid_print = 0.0
    hysteresis_counter = 0
    hysteresis_ticks = 5

    from centralised.streaming_telemetry import (
        leader_xy_increment,
        min_drone_obstacle_clearance,
        min_pairwise_xy_distance,
        pair_violation_flags,
    )

    cruise_start = time.time()
    DRONE_BODY_R = 0.12
    obs_r = stage.obstacle_diameter / 2.0
    min_pair_dist_seen = float("inf")
    pair_collision_ticks = 0
    pair_ticks_physical = 0
    obs_strict_ticks = 0
    obs_phys_ticks = 0
    leader_path_len = 0.0
    last_lp_metric_xy: Optional[Tuple[float, float]] = None
    min_dyn_clear_seen = float("inf")
    dyn_encounter_ticks = 0
    best_progress = 0.0
    pos_err_sqsum = 0.0
    pos_err_samples = 0
    formations_planned_set = frozenset({"diamond", "columnn"})
    formations_visited_set: set[str] = set()
    formation_switch_count = 0

    while True:
        t = time.time() - t_start
        if t > T_max:
            break

        try:
            pos = mission.drones[0].position if not mission.dry_run else (start_x, start_y, z)
        except Exception:
            pos = (start_x, start_y, z)
        lp_x = float(pos[0]) if pos is not None else start_x
        lp_y = float(pos[1]) if pos is not None else start_y
        dx_end = end_x - lp_x
        dy_end = end_y - lp_y
        dist_remaining = math.hypot(dx_end, dy_end)
        if dist_remaining < 1e-3:
            base_x, base_y = end_x, end_y
        else:
            step = min(lookahead_m, dist_remaining)
            base_x = lp_x + step * (dx_end / dist_remaining)
            base_y = lp_y + step * (dy_end / dist_remaining)

        # --- Swarm XY samples for merged avoidance (H2). ---
        swarm_xy: List[Tuple[float, float]] = [(lp_x, lp_y)]
        if not mission.dry_run:
            try:
                for d in mission.drones[1:]:
                    p = d.position
                    if float(p[2]) < AVOID_Z_MIN:
                        continue
                    swarm_xy.append((float(p[0]), float(p[1])))
            except Exception:
                pass

        obs = mission.obstacle_monitor.snapshot() if mission.obstacle_monitor is not None else []
        safe_eff = safe_dist + (0.45 if dist_remaining < 3.5 else 0.0)
        raw_dx_merged = 0.0
        raw_dy_merged = 0.0
        raw_mag_best = 0.0
        for sx, sy in swarm_xy:
            rx, ry = avoid_dynamic_obstacles(
                (sx, sy),
                (base_x, base_y),
                obs,
                safe_distance=safe_eff,
                lookahead_s=2.0,
                goal=(end_x, end_y),
            )
            ddx = rx - base_x
            ddy = ry - base_y
            m = math.hypot(ddx, ddy)
            if m > raw_mag_best:
                raw_mag_best = m
                raw_dx_merged = ddx
                raw_dy_merged = ddy
        raw_dx = raw_dx_merged
        raw_dy = raw_dy_merged
        raw_mag = math.hypot(raw_dx, raw_dy)
        if raw_mag > MAX_SHIFT:
            scale = MAX_SHIFT / raw_mag
            raw_dx *= scale
            raw_dy *= scale
        shift_dx_f = SHIFT_ALPHA * raw_dx + (1.0 - SHIFT_ALPHA) * shift_dx_f
        shift_dy_f = SHIFT_ALPHA * raw_dy + (1.0 - SHIFT_ALPHA) * shift_dy_f
        # Re-clamp filtered value (belt & braces).
        f_mag = math.hypot(shift_dx_f, shift_dy_f)
        if f_mag > MAX_SHIFT:
            scale = MAX_SHIFT / f_mag
            shift_dx_f *= scale
            shift_dy_f *= scale
        lx = base_x + shift_dx_f
        ly = base_y + shift_dy_f

        _AIRBORNE_Z = 0.35
        try:
            cur_lz = float(mission.drones[0].position[2]) if not mission.dry_run else z
        except Exception:
            cur_lz = z
        zs_all: List[float] = [cur_lz]
        if not mission.dry_run:
            try:
                for d in mission.drones[1:]:
                    zs_all.append(float(d.position[2]))
            except Exception:
                pass
        zs_air = [zz for zz in zs_all if zz >= _AIRBORNE_Z]
        min_z_swarm = min(zs_air) if zs_air else min(zs_all)
        if cur_lz < z_min_safe and not mission.dry_run:
            low_z_ticks += 1
        else:
            low_z_ticks = 0
        if low_z_ticks > PAUSE_TICKS:
            # Hold XY at current leader position, only command full
            # climb to cruise altitude z. This bleeds the filter state
            # toward zero so resumed chase starts smooth.
            lx = lp_x
            ly = lp_y
            shift_dx_f *= 0.5
            shift_dy_f *= 0.5
        leader_target: Vec3 = (lx, ly, z)

        fwd_end_x = lp_x + forward_lookahead_m * (dx_end / max(dist_remaining, 1e-3))
        fwd_end_y = lp_y + forward_lookahead_m * (dy_end / max(dist_remaining, 1e-3))
        nearest = None
        nearest_to_leader = None
        if obs:
            def _seg_dist(o) -> float:
                vx = fwd_end_x - lp_x
                vy = fwd_end_y - lp_y
                L2 = vx * vx + vy * vy
                if L2 < 1e-6:
                    return math.hypot(o.x - lp_x, o.y - lp_y)
                t = ((o.x - lp_x) * vx + (o.y - lp_y) * vy) / L2
                t = max(0.0, min(1.0, t))
                cx = lp_x + t * vx
                cy = lp_y + t * vy
                return math.hypot(o.x - cx, o.y - cy)
            nearest = min(_seg_dist(o) for o in obs)
            nearest_to_leader = min(math.hypot(lp_x - o.x, lp_y - o.y) for o in obs)

        # --- Formation with hysteresis. ---
        desired_compressed = nearest is not None and nearest < compress_trigger
        desired_expanded = nearest is None or nearest >= compress_trigger
        if desired_expanded and dist_remaining < 2.8:
            desired_expanded = False
        if desired_compressed and previous_formation != "columnn":
            hysteresis_counter = min(hysteresis_ticks, hysteresis_counter + 1)
            if hysteresis_counter >= hysteresis_ticks:
                formation = "columnn"
                hysteresis_counter = 0
            else:
                formation = previous_formation
        elif desired_expanded and previous_formation != "diamond":
            hysteresis_counter = min(hysteresis_ticks, hysteresis_counter + 1)
            if hysteresis_counter >= hysteresis_ticks:
                formation = "diamond"
                hysteresis_counter = 0
            else:
                formation = previous_formation
        else:
            hysteresis_counter = 0
            formation = previous_formation

        # --- Human-visible event log on stdout (rate-limited). ---
        now_s = time.time()
        if formation != previous_formation:
            formation_switch_count += 1
            mission._log(
                f"[stage4] FORMATION -> {formation.upper()} "
                f"(nearest_seg={nearest:.2f} m)" if nearest is not None
                else f"[stage4] FORMATION -> {formation.upper()}"
            )
        if f_mag > 0.25 and (now_s - last_avoid_print) > 1.0:
            nl = nearest_to_leader if nearest_to_leader is not None else float("inf")
            mission._log(
                f"[stage4] AVOIDANCE ENGAGED shift=({shift_dx_f:+.2f},{shift_dy_f:+.2f}) m | "
                f"nearest_obs={nl:.2f} m | formation={formation}"
            )
            last_avoid_print = now_s
        if low_z_ticks > PAUSE_TICKS:
            if (now_s - last_avoid_print) > 0.8:
                mission._log(
                    f"[stage4] ALTITUDE RECOVERY leader_z={cur_lz:.2f} m "
                    f"(swarm_min_z={min_z_swarm:.2f}, thr={z_min_safe} m), pausing XY"
                )
                last_avoid_print = now_s

        follower_targets = mission.controller.compute_targets(
            leader_position=leader_target,
            leader_yaw=nominal_yaw,
            formation_name=formation,
            spacing=swarm_spacing,
            previous_formation_name=previous_formation,
            transition_alpha=1.0,
            static_obstacles=None,
        )

        if not mission.dry_run:
            formations_visited_set.add(formation)
            try:
                pts: List[Tuple[float, float]] = []
                for d in mission.drones:
                    p = d.position
                    pz = float(p[2])
                    if math.isfinite(pz) and pz >= AVOID_Z_MIN:
                        px, py = float(p[0]), float(p[1])
                        if math.isfinite(px) and math.isfinite(py):
                            pts.append((px, py))
                tick_min = min_pairwise_xy_distance(pts)
                if tick_min is not None:
                    if tick_min < min_pair_dist_seen:
                        min_pair_dist_seen = tick_min
                    st, ph = pair_violation_flags(tick_min)
                    if st:
                        pair_collision_ticks += 1
                    if ph:
                        pair_ticks_physical += 1
                leader_path_len += leader_xy_increment(last_lp_metric_xy, (lp_x, lp_y))
                last_lp_metric_xy = (lp_x, lp_y)
                if obs and pts:
                    obs_xy = [(float(o.x), float(o.y)) for o in obs]
                    tick_clear = min_drone_obstacle_clearance(
                        pts, obs_xy, obs_r, DRONE_BODY_R
                    )
                    if tick_clear is not None:
                        if tick_clear < min_dyn_clear_seen:
                            min_dyn_clear_seen = tick_clear
                        if tick_clear < (obs_r + DRONE_BODY_R + 0.85):
                            dyn_encounter_ticks += 1
                        if tick_clear < 0.30:
                            obs_strict_ticks += 1
                        if tick_clear < 0.15:
                            obs_phys_ticks += 1
                for fi, ft in enumerate(follower_targets, start=1):
                    try:
                        p = mission.drones[fi].position
                        dx = float(p[0]) - float(ft[0])
                        dy = float(p[1]) - float(ft[1])
                        if math.isfinite(dx) and math.isfinite(dy):
                            pos_err_sqsum += dx * dx + dy * dy
                            pos_err_samples += 1
                    except Exception:
                        pass
            except Exception:
                pass
            if total_dist > 1e-6 and math.isfinite(dist_remaining):
                best_progress = max(best_progress, 1.0 - dist_remaining / total_dist)

            mission._stream_position(mission.drones[0], leader_target, nominal_yaw, twist_limit)
            for i, ft in enumerate(follower_targets, start=1):
                mission._stream_position(mission.drones[i], ft, nominal_yaw, twist_limit)

        tick_count += 1
        metrics.completed_waypoints = tick_count
        last_leader_target = leader_target

        previous_formation = formation

        # Terminate when actually close to end_point.
        if dist_remaining < 0.35:
            final_reached = True
            break

        time.sleep(dt_sleep)

    if not mission.dry_run:
        try:
            for d in mission.drones:
                try:
                    p = d.position
                except Exception:
                    p = None
                if p is not None:
                    mission._stream_position(d, (float(p[0]), float(p[1]), z), nominal_yaw, [0.1, 0.1, 0.1])
                elif last_leader_target is not None:
                    mission._stream_position(d, last_leader_target, nominal_yaw, [0.1, 0.1, 0.1])
        except Exception:
            pass
    metrics.planned_waypoints = tick_count
    metrics.end_time = time.time()
    metrics.cruise_time_s = max(0.0, time.time() - cruise_start)
    if min_pair_dist_seen < float("inf"):
        metrics.min_pair_distance_m = min_pair_dist_seen
    metrics.num_pair_collision_ticks = pair_collision_ticks
    metrics.num_pair_ticks_physical = pair_ticks_physical
    metrics.num_obstacle_collision_ticks = obs_strict_ticks
    metrics.num_obstacle_ticks_physical = obs_phys_ticks
    metrics.leader_path_length_m = leader_path_len
    metrics.ideal_path_length_m = total_dist
    if total_dist > 1e-6 and leader_path_len >= 0.0:
        metrics.detour_ratio = leader_path_len / total_dist
    if min_dyn_clear_seen < float("inf"):
        metrics.min_dynamic_obstacle_distance_m = min_dyn_clear_seen
    metrics.dynamic_obstacle_encounters = dyn_encounter_ticks
    metrics.formations_planned = len(formations_planned_set)
    metrics.formations_visited = len(formations_visited_set & set(formations_planned_set))
    metrics.formation_switches = formation_switch_count
    if pos_err_samples > 0:
        metrics.formation_rmse_mean = math.sqrt(pos_err_sqsum / pos_err_samples)
    cr = (1.0 if final_reached else max(0.0, min(1.0, best_progress)))
    metrics.completion_ratio = cr
    recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
    metrics.success_strict = bool(
        final_reached
        and cr >= 0.95
        and recfg >= 0.999
        and pair_collision_ticks == 0
        and obs_strict_ticks == 0
    )
    metrics.success_physical = bool(
        final_reached
        and cr >= 0.95
        and recfg >= 0.999
        and pair_ticks_physical == 0
        and obs_phys_ticks == 0
    )
    metrics.success = metrics.success_strict
    metrics.success_criterion = (
        "leader reaches end (dist<0.35m) AND completion_ratio>=0.95 AND "
        "reconfigurability>=0.999 AND inter-drone / swarm-obstacle "
        "near-miss ticks==0 (@0.30m strict / @0.15m physical)"
    )
    mission.metrics[stage_name] = metrics
    return metrics
