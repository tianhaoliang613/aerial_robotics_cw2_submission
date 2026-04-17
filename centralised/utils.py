"""Geometry utilities used by centralised mission modules."""

from __future__ import annotations

from math import atan2, cos, pi, sin
from typing import Iterable, List, Sequence, Tuple


Vec2 = Tuple[float, float]
Vec3 = Tuple[float, float, float]


def rotate_xy(x: float, y: float, yaw: float) -> Vec2:
    """Rotate a 2D vector by yaw radians."""
    return (x * cos(yaw) - y * sin(yaw), x * sin(yaw) + y * cos(yaw))


def heading_between(p0: Sequence[float], p1: Sequence[float]) -> float:
    """Compute yaw heading from p0 to p1."""
    return atan2(float(p1[1]) - float(p0[1]), float(p1[0]) - float(p0[0]))


def circle_waypoints(center: Vec2, diameter: float, num_points: int, z: float) -> List[Vec3]:
    """Generate closed-loop circular waypoints."""
    cx, cy = center
    radius = diameter / 2.0
    output: List[Vec3] = []
    for idx in range(num_points):
        theta = 2.0 * pi * (idx / num_points)
        output.append((cx + radius * cos(theta), cy + radius * sin(theta), z))
    return output


def densify_polyline(path_xy: Iterable[Vec2], step: float, z: float) -> List[Vec3]:
    """Interpolate path samples with approximately fixed XY spacing."""
    points = list(path_xy)
    if not points:
        return []
    if len(points) == 1:
        return [(points[0][0], points[0][1], z)]

    output: List[Vec3] = [(points[0][0], points[0][1], z)]
    for start, end in zip(points[:-1], points[1:]):
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        dist = (dx * dx + dy * dy) ** 0.5
        n = max(1, int(dist / max(step, 1e-3)))
        for i in range(1, n + 1):
            ratio = i / n
            output.append((start[0] + dx * ratio, start[1] + dy * ratio, z))
    return output


def interpolate_points(start: Vec3, end: Vec3, count: int) -> List[Vec3]:
    """Linear interpolation including both endpoints."""
    if count <= 1:
        return [start]
    result: List[Vec3] = []
    for i in range(count):
        ratio = i / (count - 1)
        result.append(
            (
                start[0] + (end[0] - start[0]) * ratio,
                start[1] + (end[1] - start[1]) * ratio,
                start[2] + (end[2] - start[2]) * ratio,
            )
        )
    return result

