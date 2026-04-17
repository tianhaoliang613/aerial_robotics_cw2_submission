"""Formation offset generators for 5-drone leader-follower missions."""

from __future__ import annotations

from math import cos, pi, sin
from typing import Callable, Dict, List, Tuple


Offset = Tuple[float, float, float]


def line(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    return [(-spacing * idx, 0.0, z) for idx in range(1, 5)]


def v_shape(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    return [
        (-spacing, spacing * 0.8, z),
        (-spacing, -spacing * 0.8, z),
        (-2.0 * spacing, spacing * 1.6, z),
        (-2.0 * spacing, -spacing * 1.6, z),
    ]


def diamond(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    return [
        (-spacing, 0.0, z),
        (-2.0 * spacing, spacing, z),
        (-2.0 * spacing, -spacing, z),
        (-3.0 * spacing, 0.0, z),
    ]


def square(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    return [
        (-spacing, spacing, z),
        (-spacing, -spacing, z),
        (-2.0 * spacing, spacing, z),
        (-2.0 * spacing, -spacing, z),
    ]


def grid(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    # A wider 2x2 layout than square to create visible distinction.
    return [
        (-spacing, spacing * 1.4, z),
        (-spacing, -spacing * 1.4, z),
        (-2.4 * spacing, spacing * 1.4, z),
        (-2.4 * spacing, -spacing * 1.4, z),
    ]


def orbit(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    radius = spacing * 1.6
    offsets: List[Offset] = []
    for i in range(4):
        angle = i * (2.0 * pi / 4.0)
        offsets.append((radius * cos(angle), radius * sin(angle), z))
    return offsets


def staggered(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    return [
        (-spacing, spacing * 0.8, z),
        (-1.8 * spacing, -spacing * 0.8, z),
        (-2.6 * spacing, spacing * 0.8, z),
        (-3.4 * spacing, -spacing * 0.8, z),
    ]


def column_n(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    """Strict single-file column: all followers sit directly behind the leader.

    Lateral offset is kept at zero so the swarm can be compressed through
    narrow gaps (e.g. the 1.0 m window in stage 2). The longitudinal spacing
    is clamped to <= 0.5 m so the whole 5-drone chain (tail sits ~2.0 m behind
    the leader) can fit in the limited clearance between the last wall and the
    stage boundary when crossing the last window.
    """
    s = min(spacing, 0.5)
    return [
        (-s, 0.0, z),
        (-2.0 * s, 0.0, z),
        (-3.0 * s, 0.0, z),
        (-4.0 * s, 0.0, z),
    ]


def free(spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    # Keep deterministic behavior for reproducibility.
    return diamond(spacing=spacing * 1.2, z=z)


FORMATION_REGISTRY: Dict[str, Callable[[float, float], List[Offset]]] = {
    "line": line,
    "v": v_shape,
    "v-shape": v_shape,
    "diamond": diamond,
    "square": square,
    "grid": grid,
    "orbit": orbit,
    "staggered": staggered,
    "columnn": column_n,
    "column": column_n,
    "free": free,
}


def get_formation_offsets(name: str, spacing: float = 0.7, z: float = 0.0) -> List[Offset]:
    """Return four follower offsets for the requested formation name."""
    key = name.lower()
    if key not in FORMATION_REGISTRY:
        raise ValueError(f"Unknown formation: {name}")
    return FORMATION_REGISTRY[key](spacing, z)

