"""Static/dynamic obstacle avoidance helpers for leader planning."""

from __future__ import annotations

from dataclasses import dataclass
from heapq import heappop, heappush
from math import atan2, hypot, radians, tan
from typing import Dict, Iterable, List, Sequence, Set, Tuple


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


_MAX_TRUSTED_OBSTACLE_SPEED = 1.0  # m/s; ignore clustering velocity spikes above this


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
    lookahead_s: float,
    goal: Vec2,
) -> Vec2:
    """Lateral shift toward ``next_waypoint``; only obstacles ahead in a cone toward ``goal``."""
    gx_raw = goal[0] - drone_pos[0]
    gy_raw = goal[1] - drone_pos[1]
    g_len = hypot(gx_raw, gy_raw)
    if g_len < 1e-3:
        return (next_waypoint[0], next_waypoint[1])
    gx = gx_raw / g_len
    gy = gy_raw / g_len
    px, py = -gy, gx

    cone_tan = tan(radians(58.0))
    s_ahead_min = 0.02
    s_ahead_max = min(10.0, g_len + 0.8)
    lateral_margin = 0.32
    cone_slack = 0.5
    near_along_m = 4.0

    shift_x = 0.0
    shift_y = 0.0

    def _in_forward_cone(s_along: float, c_lat: float, rad: float) -> bool:
        if s_along < s_ahead_min or s_along > s_ahead_max:
            return False
        lim = s_along * cone_tan + cone_slack + rad * 0.5
        return abs(c_lat) <= lim

    for obstacle in obstacles:
        ox, oy = _predict_obstacle_xy(obstacle, next_waypoint, lookahead_s)
        min_d = safe_distance + obstacle.radius

        tox = ox - drone_pos[0]
        toy = oy - drone_pos[1]
        s_along = tox * gx + toy * gy
        c_lat = tox * px + toy * py
        d_d0 = hypot(tox, toy)
        in_cone = _in_forward_cone(s_along, c_lat, obstacle.radius)
        critical_near = d_d0 < (obstacle.radius + min_d * 0.42) and s_along > -0.08
        if not in_cone and not critical_near:
            continue

        d_line = abs(c_lat)
        lateral_full = min_d + lateral_margin
        lateral_tight = max(obstacle.radius * 2.5, 0.52) + 0.15
        t_along = max(0.0, min(1.0, 1.0 - s_along / max(near_along_m, 1e-3)))
        eff_clear = lateral_tight + (lateral_full - lateral_tight) * t_along
        if in_cone and d_line < eff_clear:
            overlap = (eff_clear - d_line) + 0.12
            if overlap <= 0.0:
                overlap = 0.0
            if abs(c_lat) < 1e-2:
                sp = hypot(obstacle.vx, obstacle.vy)
                if sp > 1e-3 and sp <= _MAX_TRUSTED_OBSTACLE_SPEED:
                    c_v = obstacle.vx * px + obstacle.vy * py
                    side = 1.0 if c_v > 0 else -1.0
                else:
                    side = 1.0
            else:
                side = 1.0 if c_lat > 0 else -1.0
            w = min(overlap, 1.05)
            shift_x += -side * px * w
            shift_y += -side * py * w

        d_d = d_d0
        em_r = min_d + 0.55
        if d_d < em_r and d_d > 1e-4 and (in_cone or critical_near):
            rdx = (drone_pos[0] - ox) / d_d
            rdy = (drone_pos[1] - oy) / d_d
            along = rdx * gx + rdy * gy
            lx = rdx - along * gx
            ly = rdy - along * gy
            lnorm = hypot(lx, ly)
            if lnorm > 1e-3:
                lx /= lnorm
                ly /= lnorm
                overlap_e = (em_r - d_d) + 0.2
                shift_x += lx * overlap_e * 1.05
                shift_y += ly * overlap_e * 1.05

    mag = hypot(shift_x, shift_y)
    cap = 2.8
    if mag > cap and mag > 1e-6:
        scale = cap / mag
        shift_x *= scale
        shift_y *= scale

    return (next_waypoint[0] + shift_x, next_waypoint[1] + shift_y)

