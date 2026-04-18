"""Leader waypoint planners for all four CW2 stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

from .obstacle_avoidance import astar_plan, smooth_path
from .scenario_loader import ScenarioSpec, WindowSpec
from .utils import Vec3, circle_waypoints, densify_polyline, interpolate_points


@dataclass
class StagePlan:
    """Planned waypoints plus optional per-waypoint formation directives."""

    waypoints: List[Vec3]
    formation_names: List[str]


class LeaderPlanner:
    """Build trajectory and formation schedule for each stage."""

    def __init__(self, scenario: ScenarioSpec, cruise_height: float = 1.2) -> None:
        self.scenario = scenario
        self.cruise_height = cruise_height

    def _stage1_formations(self, raw_names: Sequence[str]) -> List[str]:
        normalize = {
            "v-shape": "v",
            "square": "diamond",
            "columnn": "line",
            "column": "line",
            "free": "staggered",
        }
        preferred = {"line", "v", "diamond", "orbit", "grid", "staggered"}
        output: List[str] = []
        for name in raw_names:
            key = normalize.get(name.lower(), name.lower())
            if key in preferred and key not in output:
                output.append(key)
        if not output:
            output = ["line", "v", "diamond", "orbit", "grid", "staggered"]
        return output

    def plan_stage1(self, num_points: int = 24) -> StagePlan:
        stage = self.scenario.stage1
        if stage is None:
            return StagePlan([], [])
        waypoints = circle_waypoints(stage.stage_center, stage.diameter, num_points=num_points, z=self.cruise_height)
        stage1_forms = self._stage1_formations(stage.formations)
        segment = max(1, len(waypoints) // len(stage1_forms))
        formation_schedule: List[str] = []
        for idx in range(len(waypoints)):
            name = stage1_forms[min(idx // segment, len(stage1_forms) - 1)]
            formation_schedule.append(name)
        return StagePlan(waypoints, formation_schedule)

    def _window_transition_waypoints(self, window: WindowSpec, approach_offset: float, cross_direction: int) -> List[Vec3]:
        """Pre / through / post waypoints for one window (traverse along ±y)."""
        wx, wy = window.center_global_xy
        wz = window.pass_height
        sign = 1 if cross_direction >= 0 else -1
        pre = (wx, wy - sign * approach_offset, wz)
        inside = (wx, wy, wz)
        post = (wx, wy + sign * approach_offset, wz)
        return [pre, inside, post]

    def plan_stage2(self) -> StagePlan:
        stage = self.scenario.stage2
        if stage is None:
            return StagePlan([], [])

        route: List[Vec3] = []
        formations: List[str] = []
        half_y = self.scenario.stage_size[1] / 2.0
        stage_min_y = stage.stage_center[1] - half_y
        stage_max_y = stage.stage_center[1] + half_y

        # Sort walls by world-y descending and traverse from north (+y) to south (-y).
        sorted_windows = sorted(stage.windows, key=lambda w: -w.center_global_xy[1])
        cross_direction = -1  # moving toward -y

        entry_y = stage_max_y - 0.8
        entry_x = sorted_windows[0].center_global_xy[0] if sorted_windows else stage.stage_center[0]
        entry = (entry_x, entry_y, self.cruise_height)
        route.append(entry)
        formations.append("diamond")

        last_idx = len(sorted_windows) - 1
        for idx_w, window in enumerate(sorted_windows):
            transitions = self._window_transition_waypoints(window, approach_offset=1.1, cross_direction=cross_direction)
            for point in transitions:
                route.append(point)
                formations.append("columnn")
            if idx_w != last_idx:
                post_x, post_y, post_z = transitions[-1]
                next_window = sorted_windows[idx_w + 1]
                next_wy = float(next_window.center_global_xy[1])
                next_pre_y = next_wy - cross_direction * 1.1
                corridor = abs(next_pre_y - post_y)
                desired_hold = 2.2
                hold_offset = min(desired_hold, max(0.0, corridor - 0.2))
                if hold_offset > 0.05:
                    hold_y = post_y + cross_direction * hold_offset
                    route.append((post_x, hold_y, post_z))
                    formations.append("columnn")
                    hold_anchor_y = hold_y
                else:
                    hold_anchor_y = post_y
                remaining = abs(next_pre_y - hold_anchor_y)
                if remaining >= 1.5:
                    reform_offset = min(1.0, remaining - 0.5)
                    reform_y = hold_anchor_y + cross_direction * reform_offset
                    route.append((post_x, reform_y, self.cruise_height))
                    formations.append("diamond")
            else:
                post_x, post_y, pass_height = transitions[-1]
                clearance_y = post_y + cross_direction * 3.5
                if cross_direction < 0:
                    clearance_y = max(clearance_y, stage_min_y + 0.05)
                else:
                    clearance_y = min(clearance_y, stage_max_y - 0.05)
                route.append((post_x, clearance_y, pass_height))
                formations.append("columnn")
                if cross_direction < 0:
                    room_ahead = max(0.0, clearance_y - (stage_min_y + 0.1))
                else:
                    room_ahead = max(0.0, (stage_max_y - 0.1) - clearance_y)
                descent_step = min(0.8, room_ahead)
                descent_y = clearance_y + cross_direction * descent_step
                route.append((post_x, descent_y, self.cruise_height))
                formations.append("columnn")

        return StagePlan(route, formations)

    def plan_stage3(self) -> StagePlan:
        """Forest: single-file columnn along A* + densified path."""
        stage = self.scenario.stage3
        if stage is None:
            return StagePlan([], [])

        start_x, start_y = stage.start_point_global
        end_x, end_y = stage.end_point_global
        direction = 1.0 if end_x >= start_x else -1.0

        padding = 0.2
        half_x = self.scenario.stage_size[0] / 2.0
        half_y = self.scenario.stage_size[1] / 2.0
        stage_min_x = stage.stage_center[0] - half_x + padding
        stage_max_x = stage.stage_center[0] + half_x - padding
        stage_min_y = stage.stage_center[1] - half_y + padding
        stage_max_y = stage.stage_center[1] + half_y - padding

        leader_safety = (stage.obstacle_diameter / 2.0) + 0.35
        obstacles_for_leader = [(x, y, leader_safety) for (x, y) in stage.obstacles_global]
        path_xy = astar_plan(
            start=stage.start_point_global,
            goal=stage.end_point_global,
            obstacles=obstacles_for_leader,
            bounds_min=(stage_min_x, stage_min_y),
            bounds_max=(stage_max_x, stage_max_y),
            resolution=0.30,
        )
        smooth_xy = smooth_path(path_xy)
        forest_wps = densify_polyline(smooth_xy, step=0.45, z=self.cruise_height)

        rally_in = (start_x, start_y, self.cruise_height)
        tail_clear_x = max(stage_min_x, min(stage_max_x, end_x + direction * 0.6))
        tail_clear = (tail_clear_x, end_y, self.cruise_height)

        forest_body = (
            forest_wps[1:] if forest_wps and forest_wps[0][:2] == stage.start_point_global else forest_wps
        )

        waypoints: List[Vec3] = [rally_in] + list(forest_body) + [tail_clear]
        formations: List[str] = ["columnn"] * len(waypoints)
        return StagePlan(waypoints, formations)

    def plan_stage4(self) -> StagePlan:
        """Nominal straight leader path at cruise height (avoidance is online in stage4)."""
        stage = self.scenario.stage4
        if stage is None:
            return StagePlan([], [])
        base = interpolate_points(
            (stage.start_point_global[0], stage.start_point_global[1], self.cruise_height),
            (stage.end_point_global[0], stage.end_point_global[1], self.cruise_height),
            count=9,
        )
        formations = ["diamond" for _ in base]
        return StagePlan(list(base), formations)

