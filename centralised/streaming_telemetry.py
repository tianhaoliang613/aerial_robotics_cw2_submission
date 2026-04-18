"""Pure helpers for streaming-stage telemetry (unit-testable, no ROS)."""

from __future__ import annotations

import math
from typing import List, Optional, Sequence, Tuple

PAIR_SAFETY_M = 0.30
PAIR_PHYSICAL_M = 0.15


def min_pairwise_xy_distance(xy: Sequence[Tuple[float, float]]) -> Optional[float]:
    """Minimum Euclidean XY distance over all unordered pairs; None if <2 points."""
    n = len(xy)
    if n < 2:
        return None
    best = float("inf")
    for i in range(n):
        for j in range(i + 1, n):
            d = math.hypot(xy[i][0] - xy[j][0], xy[i][1] - xy[j][1])
            if d < best:
                best = d
    return best


def pair_violation_flags(min_dist: float) -> Tuple[bool, bool]:
    """Return (strict_tick, physical_tick) for one sample's minimum pair distance."""
    return (min_dist < PAIR_SAFETY_M, min_dist < PAIR_PHYSICAL_M)


def leader_xy_increment(
    prev_xy: Optional[Tuple[float, float]],
    cur_xy: Tuple[float, float],
) -> float:
    """Segment length along leader path; 0 if prev is None or non-finite."""
    if prev_xy is None:
        return 0.0
    px, py = prev_xy
    cx, cy = cur_xy
    if not all(map(math.isfinite, (px, py, cx, cy))):
        return 0.0
    return math.hypot(cx - px, cy - py)


def min_drone_obstacle_clearance(
    drone_xy: Sequence[Tuple[float, float]],
    obstacle_xy: Sequence[Tuple[float, float]],
    obstacle_radius_m: float,
    drone_body_radius_m: float,
) -> Optional[float]:
    """Minimum (center_dist - obs_r - drone_r) over all pairs; None if empty."""
    if not drone_xy or not obstacle_xy:
        return None
    best = float("inf")
    for qx, qy in drone_xy:
        for ox, oy in obstacle_xy:
            dc = math.hypot(qx - ox, qy - oy) - obstacle_radius_m - drone_body_radius_m
            if dc < best:
                best = dc
    return best
