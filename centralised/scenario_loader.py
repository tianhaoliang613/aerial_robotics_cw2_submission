"""Load and normalize CW2 scenario configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml


Vec2 = Tuple[float, float]
Vec3 = Tuple[float, float, float]


@dataclass(frozen=True)
class WindowSpec:
    """Window parameters in both local and global coordinates.

    NOTE on YAML layout: despite the comment ``# y, x`` inside
    ``scenarios/*.yaml``, the two entries of ``window.center`` are consumed by
    ``utils/generate_world_from_scenario.py`` as ``[local_x, local_y]`` where
    ``local_x`` is the opening offset along the wall (world +x because the wall
    is not rotated) and ``local_y`` is the wall-origin offset along world y.
    The geometry: a wall lies at ``y = stage_center_y + local_y`` and extends
    along world x over ``stage_size[1]`` metres with thickness along y,
    therefore drones must traverse along the y axis through the opening at
    ``x = stage_center_x + local_x``.
    """

    window_id: str
    center_local_xy: Vec2
    center_global_xy: Vec2
    gap_width: float
    distance_floor: float
    height: float
    thickness: float

    @property
    def pass_height(self) -> float:
        """Recommended z value to pass through this window center."""
        return self.distance_floor + (self.height / 2.0)


@dataclass(frozen=True)
class Stage1Spec:
    stage_center: Vec2
    diameter: float
    formations: List[str]


@dataclass(frozen=True)
class Stage2Spec:
    stage_center: Vec2
    room_height: float
    windows: List[WindowSpec]


@dataclass(frozen=True)
class Stage3Spec:
    stage_center: Vec2
    start_point_local: Vec2
    end_point_local: Vec2
    start_point_global: Vec2
    end_point_global: Vec2
    obstacle_height: float
    obstacle_diameter: float
    obstacles_local: List[Vec2]
    obstacles_global: List[Vec2]


@dataclass(frozen=True)
class Stage4Spec:
    stage_center: Vec2
    start_point_local: Vec2
    end_point_local: Vec2
    start_point_global: Vec2
    end_point_global: Vec2
    num_obstacles: int
    obstacle_velocity: float
    obstacle_height: float
    obstacle_diameter: float


@dataclass(frozen=True)
class ScenarioSpec:
    """Top-level scenario object consumed by planner/controller."""

    name: str
    stage_size: Vec2
    drone_start_pose: Vec3
    stage1: Optional[Stage1Spec]
    stage2: Optional[Stage2Spec]
    stage3: Optional[Stage3Spec]
    stage4: Optional[Stage4Spec]


def _vec2(values: List[float]) -> Vec2:
    return (float(values[0]), float(values[1]))


def _global_xy(stage_center: Vec2, local_xy: Vec2) -> Vec2:
    return (stage_center[0] + local_xy[0], stage_center[1] + local_xy[1])


def _parse_stage1(raw: Dict) -> Stage1Spec:
    return Stage1Spec(
        stage_center=_vec2(raw["stage_center"]),
        diameter=float(raw["trajectory"]["diameter"]),
        formations=[str(name).lower() for name in raw.get("formations", [])],
    )


def _parse_stage2(raw: Dict) -> Stage2Spec:
    center = _vec2(raw["stage_center"])
    windows: List[WindowSpec] = []

    for window_id, window in raw["windows"].items():
        # The YAML comment says "y, x" but generate_world_from_scenario.py
        # consumes the pair as (local_x_on_wall, local_y_of_wall_origin).
        center_local_xy = _vec2(window["center"])
        center_global_xy = (
            center[0] + center_local_xy[0],
            center[1] + center_local_xy[1],
        )
        windows.append(
            WindowSpec(
                window_id=str(window_id),
                center_local_xy=center_local_xy,
                center_global_xy=center_global_xy,
                gap_width=float(window["gap_width"]),
                distance_floor=float(window["distance_floor"]),
                height=float(window["height"]),
                thickness=float(window["thickness"]),
            )
        )

    windows.sort(key=lambda spec: spec.window_id)
    return Stage2Spec(
        stage_center=center,
        room_height=float(raw["room_height"]),
        windows=windows,
    )


def _parse_stage3(raw: Dict) -> Stage3Spec:
    center = _vec2(raw["stage_center"])
    start_local = _vec2(raw["start_point"])
    end_local = _vec2(raw["end_point"])
    obstacles_local = [_vec2(values) for values in raw.get("obstacles", [])]
    obstacles_global = [_global_xy(center, item) for item in obstacles_local]
    return Stage3Spec(
        stage_center=center,
        start_point_local=start_local,
        end_point_local=end_local,
        start_point_global=_global_xy(center, start_local),
        end_point_global=_global_xy(center, end_local),
        obstacle_height=float(raw["obstacle_height"]),
        obstacle_diameter=float(raw["obstacle_diameter"]),
        obstacles_local=obstacles_local,
        obstacles_global=obstacles_global,
    )


def _parse_stage4(raw: Dict) -> Stage4Spec:
    center = _vec2(raw["stage_center"])
    start_local = _vec2(raw["start_point"])
    end_local = _vec2(raw["end_point"])
    return Stage4Spec(
        stage_center=center,
        start_point_local=start_local,
        end_point_local=end_local,
        start_point_global=_global_xy(center, start_local),
        end_point_global=_global_xy(center, end_local),
        num_obstacles=int(raw["num_obstacles"]),
        obstacle_velocity=float(raw["obstacle_velocity"]),
        obstacle_height=float(raw["obstacle_height"]),
        obstacle_diameter=float(raw["obstacle_diameter"]),
    )


def load_world_drone_spawns_by_model_name(world_path: Path) -> Dict[str, Tuple[float, float, float]]:
    """Parse ``drones:`` entries from a Gazebo world YAML (e.g. world_swarm_stage4).

    Returns ``model_name -> (x, y, z)`` in earth frame as listed under ``xyz``.
    """
    with world_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    out: Dict[str, Tuple[float, float, float]] = {}
    for entry in raw.get("drones", []) or []:
        name = entry.get("model_name")
        xyz = entry.get("xyz")
        if not name or not xyz or len(xyz) < 3:
            continue
        out[str(name)] = (float(xyz[0]), float(xyz[1]), float(xyz[2]))
    return out


def load_scenario(path: str) -> ScenarioSpec:
    """Load scenario yaml and convert into typed configuration objects."""
    scenario_path = Path(path)
    with scenario_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    start_pose = raw.get("drone_start_pose", {"x": 0.0, "y": 0.0, "z": 0.0})
    return ScenarioSpec(
        name=str(raw.get("name", scenario_path.stem)),
        stage_size=_vec2(raw["stage_size"]),
        drone_start_pose=(
            float(start_pose.get("x", 0.0)),
            float(start_pose.get("y", 0.0)),
            float(start_pose.get("z", 0.0)),
        ),
        stage1=_parse_stage1(raw["stage1"]) if "stage1" in raw else None,
        stage2=_parse_stage2(raw["stage2"]) if "stage2" in raw else None,
        stage3=_parse_stage3(raw["stage3"]) if "stage3" in raw else None,
        stage4=_parse_stage4(raw["stage4"]) if "stage4" in raw else None,
    )

