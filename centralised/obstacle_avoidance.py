"""Static/dynamic obstacle avoidance helpers for leader planning."""

from __future__ import annotations

from dataclasses import dataclass
from heapq import heappop, heappush
from math import atan2, cos, hypot, sin
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


Vec2 = Tuple[float, float]


@dataclass(frozen=True)
class DynamicObstacle:
    """Simple obstacle state used by reactive stage4 planning."""

    x: float
    y: float
    vx: float
    vy: float
    radius: float


def _to_grid(point: Vec2, origin: Vec2, resolution: float) -> Tuple[int, int]:
    return (int(round((point[0] - origin[0]) / resolution)), int(round((point[1] - origin[1]) / resolution)))


def _to_world(node: Tuple[int, int], origin: Vec2, resolution: float) -> Vec2:
    return (origin[0] + node[0] * resolution, origin[1] + node[1] * resolution)


def _heuristic(a: Tuple[int, int], b: Tuple[int, int]) -> float:
    return hypot(a[0] - b[0], a[1] - b[1])


def _neighbors(node: Tuple[int, int]) -> Iterable[Tuple[int, int]]:
    x, y = node
    for dx, dy in (
        (1, 0),
        (-1, 0),
        (0, 1),
        (0, -1),
        (1, 1),
        (1, -1),
        (-1, 1),
        (-1, -1),
    ):
        yield (x + dx, y + dy)


def _collision(point: Vec2, obstacles: Sequence[Tuple[float, float, float]]) -> bool:
    px, py = point
    for ox, oy, radius in obstacles:
        if hypot(px - ox, py - oy) <= radius:
            return True
    return False


def astar_plan(
    start: Vec2,
    goal: Vec2,
    obstacles: Sequence[Tuple[float, float, float]],
    bounds_min: Vec2,
    bounds_max: Vec2,
    resolution: float = 0.35,
) -> List[Vec2]:
    """Plan a collision-free 2D path over a bounded occupancy grid."""
    origin = bounds_min
    start_grid = _to_grid(start, origin, resolution)
    goal_grid = _to_grid(goal, origin, resolution)

    if _collision(start, obstacles) or _collision(goal, obstacles):
        return [start, goal]

    open_heap: List[Tuple[float, Tuple[int, int]]] = []
    heappush(open_heap, (0.0, start_grid))
    came_from: Dict[Tuple[int, int], Tuple[int, int]] = {}
    g_costs: Dict[Tuple[int, int], float] = {start_grid: 0.0}
    visited: Set[Tuple[int, int]] = set()

    while open_heap:
        _, current = heappop(open_heap)
        if current in visited:
            continue
        visited.add(current)
        if current == goal_grid:
            break

        for nxt in _neighbors(current):
            world = _to_world(nxt, origin, resolution)
            if not (bounds_min[0] <= world[0] <= bounds_max[0] and bounds_min[1] <= world[1] <= bounds_max[1]):
                continue
            if _collision(world, obstacles):
                continue
            step_cost = hypot(nxt[0] - current[0], nxt[1] - current[1])
            tentative = g_costs[current] + step_cost
            if tentative >= g_costs.get(nxt, float("inf")):
                continue
            came_from[nxt] = current
            g_costs[nxt] = tentative
            score = tentative + _heuristic(nxt, goal_grid)
            heappush(open_heap, (score, nxt))

    if goal_grid not in came_from and goal_grid != start_grid:
        return [start, goal]

    path_grid: List[Tuple[int, int]] = [goal_grid]
    cursor = goal_grid
    while cursor != start_grid:
        cursor = came_from[cursor]
        path_grid.append(cursor)
    path_grid.reverse()
    return [_to_world(node, origin, resolution) for node in path_grid]


def smooth_path(path_xy: Sequence[Vec2]) -> List[Vec2]:
    """Remove collinear points from an XY path."""
    if len(path_xy) < 3:
        return list(path_xy)
    result: List[Vec2] = [path_xy[0]]
    for idx in range(1, len(path_xy) - 1):
        p_prev = result[-1]
        p_now = path_xy[idx]
        p_next = path_xy[idx + 1]
        angle1 = atan2(p_now[1] - p_prev[1], p_now[0] - p_prev[0])
        angle2 = atan2(p_next[1] - p_now[1], p_next[0] - p_now[0])
        if abs(angle1 - angle2) < 1e-2:
            continue
        result.append(p_now)
    result.append(path_xy[-1])
    return result


# Upper bound on any obstacle speed we will trust for forward prediction.
# The simulator's obstacle plugin runs at movement_velocity=0.5 m/s, so any
# estimate above ~1.0 m/s is almost certainly a mis-association artefact
# from the spatial-clustering monitor (we have observed velocity spikes
# of 140+ m/s from cross-track swaps). Rather than scaling the direction
# (which keeps a random direction from the spike), we simply DROP
# untrusted estimates to zero -- safer, because the obstacle's current
# position is always known and treating it as momentarily stationary
# still yields the correct lateral deflection from the segment push.
_MAX_TRUSTED_OBSTACLE_SPEED = 1.0


def _predict_obstacle_xy(
    obstacle: DynamicObstacle, next_waypoint: Vec2, lookahead_s: float
) -> Vec2:
    """Pick current vs short-horizon predicted position (closer to waypoint)."""
    speed_mag = hypot(obstacle.vx, obstacle.vy)
    if speed_mag > _MAX_TRUSTED_OBSTACLE_SPEED:
        vx, vy = 0.0, 0.0
    else:
        vx, vy = obstacle.vx, obstacle.vy
    ox_cur = obstacle.x
    oy_cur = obstacle.y
    ox_pred = obstacle.x + vx * lookahead_s
    oy_pred = obstacle.y + vy * lookahead_s
    d_cur = hypot(next_waypoint[0] - ox_cur, next_waypoint[1] - oy_cur)
    d_pred = hypot(next_waypoint[0] - ox_pred, next_waypoint[1] - oy_pred)
    if d_cur <= d_pred:
        return (ox_cur, oy_cur)
    return (ox_pred, oy_pred)


def avoid_dynamic_obstacles(
    drone_pos: Vec2,
    next_waypoint: Vec2,
    obstacles: Sequence[DynamicObstacle],
    safe_distance: float,
    lookahead_s: float = 0.6,
    goal: Optional[Vec2] = None,
) -> Vec2:
    """Reactive waypoint shift that deflects around nearby moving obstacles.

    When ``goal`` is provided (stage4 recommended), deflection uses the
    **mission corridor** drone → goal: only lateral offsets perpendicular to
    that axis are applied. This avoids the old "drone → 2.5 m lookahead"
    segment whose normal **rotates every tick**, which produced erratic
    setpoints and sometimes a net shift that looked like flying **toward** a
    pillar.

    When ``goal`` is ``None``, falls back to the legacy segment-based rule for
    backward compatibility.

    NOTE: Returns the **shifted waypoint** only; the drone's own position is
    unchanged.
    """
    if goal is not None:
        return _avoid_goal_corridor(
            drone_pos, next_waypoint, obstacles, safe_distance, lookahead_s, goal
        )
    return _avoid_segment_legacy(
        drone_pos, next_waypoint, obstacles, safe_distance, lookahead_s
    )


def _avoid_goal_corridor(
    drone_pos: Vec2,
    next_waypoint: Vec2,
    obstacles: Sequence[DynamicObstacle],
    safe_distance: float,
    lookahead_s: float,
    goal: Vec2,
) -> Vec2:
    """Lateral-only avoidance w.r.t. the line drone → goal (stable normal)."""
    gx_raw = goal[0] - drone_pos[0]
    gy_raw = goal[1] - drone_pos[1]
    g_len = hypot(gx_raw, gy_raw)
    if g_len < 1e-3:
        return (next_waypoint[0], next_waypoint[1])
    gx = gx_raw / g_len
    gy = gy_raw / g_len
    # Left normal to forward (mission axis).
    px, py = -gy, gx

    shift_x = 0.0
    shift_y = 0.0
    s_min = -0.4
    s_max = 12.0
    lateral_margin = 0.35

    for obstacle in obstacles:
        ox, oy = _predict_obstacle_xy(obstacle, next_waypoint, lookahead_s)
        min_d = safe_distance + obstacle.radius

        tox = ox - drone_pos[0]
        toy = oy - drone_pos[1]
        s_along = tox * gx + toy * gy
        c_lat = tox * px + toy * py
        if s_along < s_min or s_along > s_max:
            continue

        d_line = abs(c_lat)
        if d_line < min_d + lateral_margin:
            overlap = (min_d + lateral_margin - d_line) + 0.15
            if overlap <= 0.0:
                overlap = 0.0
            # Obstacle on +c_lat side of corridor → steer waypoint to -p side.
            if abs(c_lat) < 1e-2:
                sp = hypot(obstacle.vx, obstacle.vy)
                if sp > 1e-3 and sp <= _MAX_TRUSTED_OBSTACLE_SPEED:
                    c_v = obstacle.vx * px + obstacle.vy * py
                    # Obstacle sliding in +p → pass on the -p side (side=+1).
                    side = 1.0 if c_v > 0 else -1.0
                else:
                    side = 1.0
            else:
                side = 1.0 if c_lat > 0 else -1.0
            w = min(overlap, 1.1)
            shift_x += -side * px * w
            shift_y += -side * py * w

        # Emergency: very close to drone — push only in the plane ⟂ goal
        # (no component along -g that would command "back up into" clutter).
        d_d = hypot(tox, toy)
        em_r = min_d + 0.65
        if d_d < em_r and d_d > 1e-4:
            rdx = (drone_pos[0] - ox) / d_d
            rdy = (drone_pos[1] - oy) / d_d
            along = rdx * gx + rdy * gy
            lx = rdx - along * gx
            ly = rdy - along * gy
            lnorm = hypot(lx, ly)
            if lnorm > 1e-3:
                lx /= lnorm
                ly /= lnorm
                overlap_e = (em_r - d_d) + 0.25
                shift_x += lx * overlap_e * 1.1
                shift_y += ly * overlap_e * 1.1

    # Cap combined lateral impulse so five pillars do not explode the shift.
    mag = hypot(shift_x, shift_y)
    cap = 2.8
    if mag > cap and mag > 1e-6:
        scale = cap / mag
        shift_x *= scale
        shift_y *= scale

    return (next_waypoint[0] + shift_x, next_waypoint[1] + shift_y)


def _avoid_segment_legacy(
    drone_pos: Vec2,
    next_waypoint: Vec2,
    obstacles: Sequence[DynamicObstacle],
    safe_distance: float,
    lookahead_s: float,
) -> Vec2:
    """Legacy segment drone→waypoint avoidance (no global goal axis)."""
    dx_seg = next_waypoint[0] - drone_pos[0]
    dy_seg = next_waypoint[1] - drone_pos[1]
    seg_len = hypot(dx_seg, dy_seg)
    if seg_len < 1e-3:
        seg_dir_x, seg_dir_y = 1.0, 0.0
    else:
        seg_dir_x = dx_seg / seg_len
        seg_dir_y = dy_seg / seg_len
    perp_x = -seg_dir_y
    perp_y = seg_dir_x

    shift_x = 0.0
    shift_y = 0.0
    for obstacle in obstacles:
        ox, oy = _predict_obstacle_xy(obstacle, next_waypoint, lookahead_s)
        min_distance = safe_distance + obstacle.radius

        if seg_len > 1e-3:
            t_proj = ((ox - drone_pos[0]) * dx_seg + (oy - drone_pos[1]) * dy_seg) / (
                seg_len * seg_len
            )
            t_clamped = max(0.0, min(1.0, t_proj))
            closest_x = drone_pos[0] + t_clamped * dx_seg
            closest_y = drone_pos[1] + t_clamped * dy_seg
            seg_dist = hypot(closest_x - ox, closest_y - oy)
            if seg_dist < min_distance and t_proj > -0.1 and t_proj < 1.2:
                side_cross = (ox - drone_pos[0]) * perp_x + (oy - drone_pos[1]) * perp_y
                side = 1.0 if side_cross >= 0 else -1.0
                overlap = (min_distance - seg_dist) + 0.3
                shift_x += -side * perp_x * overlap
                shift_y += -side * perp_y * overlap

        dx_d = drone_pos[0] - ox
        dy_d = drone_pos[1] - oy
        d_d = hypot(dx_d, dy_d)
        emergency_dist = min_distance + 0.5
        if d_d < emergency_dist:
            if d_d < 1e-3:
                push_x, push_y = perp_x, perp_y
            else:
                push_x, push_y = dx_d / d_d, dy_d / d_d
            overlap_d = (emergency_dist - d_d) + 0.4
            shift_x += push_x * overlap_d
            shift_y += push_y * overlap_d

    return (next_waypoint[0] + shift_x, next_waypoint[1] + shift_y)

