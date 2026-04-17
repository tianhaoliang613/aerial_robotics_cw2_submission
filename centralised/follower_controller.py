"""Follower target generation for centralised leader-follower control."""

from __future__ import annotations

from dataclasses import dataclass
from math import hypot
from typing import List, Optional, Sequence, Tuple

from .formations import Offset, get_formation_offsets
from .utils import rotate_xy


Vec2 = Tuple[float, float]
Vec3 = Tuple[float, float, float]


@dataclass
class FollowerController:
    """Compute follower goals from leader state and desired formation."""

    follower_count: int = 4
    default_spacing: float = 0.75
    default_altitude: float = 1.2
    collision_margin: float = 0.30

    def _is_safe_point(self, point: Vec3, static_obstacles: Sequence[Tuple[float, float, float]]) -> bool:
        px, py, _ = point
        for ox, oy, radius in static_obstacles:
            if hypot(px - ox, py - oy) <= radius + self.collision_margin:
                return False
        return True

    def _blend_offsets(self, previous: List[Offset], current: List[Offset], blend: float) -> List[Offset]:
        blend = max(0.0, min(1.0, blend))
        output: List[Offset] = []
        for prev, cur in zip(previous, current):
            output.append(
                (
                    prev[0] + (cur[0] - prev[0]) * blend,
                    prev[1] + (cur[1] - prev[1]) * blend,
                    prev[2] + (cur[2] - prev[2]) * blend,
                )
            )
        return output

    def compute_targets(
        self,
        leader_position: Vec3,
        leader_yaw: float,
        formation_name: str,
        spacing: Optional[float] = None,
        previous_formation_name: Optional[str] = None,
        transition_alpha: float = 1.0,
        static_obstacles: Optional[Sequence[Tuple[float, float, float]]] = None,
    ) -> List[Vec3]:
        """Return follower target waypoints in global frame."""
        spacing = spacing if spacing is not None else self.default_spacing
        offsets = get_formation_offsets(formation_name, spacing=spacing, z=0.0)
        if previous_formation_name:
            previous = get_formation_offsets(previous_formation_name, spacing=spacing, z=0.0)
            offsets = self._blend_offsets(previous, offsets, transition_alpha)

        static_obstacles = static_obstacles or []
        leader_x, leader_y, leader_z = leader_position
        targets: List[Vec3] = []

        for offset in offsets[: self.follower_count]:
            dx, dy = rotate_xy(offset[0], offset[1], leader_yaw)
            target = (leader_x + dx, leader_y + dy, leader_z + offset[2])
            if not self._is_safe_point(target, static_obstacles):
                # Fallback to compressed column when a target intersects a static obstacle.
                compressed_offsets = get_formation_offsets("columnn", spacing=spacing * 0.8, z=0.0)
                alt = compressed_offsets[len(targets)]
                cdx, cdy = rotate_xy(alt[0], alt[1], leader_yaw)
                target = (leader_x + cdx, leader_y + cdy, leader_z + alt[2])
            targets.append(target)
        return targets

