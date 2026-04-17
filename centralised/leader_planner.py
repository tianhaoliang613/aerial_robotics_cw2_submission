"""Leader waypoint planners for all four CW2 stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .obstacle_avoidance import DynamicObstacle, astar_plan, avoid_dynamic_obstacles, smooth_path
from .scenario_loader import ScenarioSpec, Stage2Spec, WindowSpec
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

    def _window_transition_waypoints(self, stage2: Stage2Spec, window: WindowSpec, approach_offset: float, cross_direction: int) -> List[Vec3]:
        """Approach / inside / depart waypoints for crossing one wall.

        The wall extends along world x and has thickness along world y, so the
        drones must traverse along y. ``cross_direction`` is +1 when flying
        toward +y, -1 when flying toward -y.
        """
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
        # Start directly in column so the swarm never needs the extra
        # diamond->column transition before the first window (which the user
        # observed as unnecessary jitter at the entry).
        formations.append("columnn")

        last_idx = len(sorted_windows) - 1
        for idx_w, window in enumerate(sorted_windows):
            transitions = self._window_transition_waypoints(stage, window, approach_offset=1.1, cross_direction=cross_direction)
            for idx, point in enumerate(transitions):
                route.append(point)
                # Hold the column longer: keep columnn through pre/inside/post
                # so the whole chain is lined up with the narrow gap; the
                # "line" only differs laterally by zero anyway (since column_n
                # is strict single-file), but keeping the same name avoids
                # transition blending jitter at the post point.
                formations.append("columnn")
            # Re-form after leaving the wall, but skip the reform for the last
            # window (go straight to the clearance/exit instead of overshooting).
            if idx_w != last_idx:
                reform = (transitions[-1][0], transitions[-1][1] + cross_direction * 0.9, self.cruise_height)
                route.append(reform)
                formations.append("diamond")
            else:
                # Last window: keep the column at pass_height until the trailing
                # drone (~2.0 m behind the leader with clamped spacing 0.5 m)
                # has also cleared the wall. Only then descend -- still in
                # column -- so nobody crosses the wall at a z below the gap.
                post_x, post_y, pass_height = transitions[-1]
                # Push the leader further past the wall so the trailing drone
                # also gets a comfortable margin. Hug the stage boundary
                # (only 0.05 m inset) rather than the earlier 0.2 m, and go
                # beyond the strict chain-length estimate (2 m) to 3.0 m.
                clearance_y = post_y + cross_direction * 3.0
                if cross_direction < 0:
                    clearance_y = max(clearance_y, stage_min_y + 0.05)
                else:
                    clearance_y = min(clearance_y, stage_max_y - 0.05)
                route.append((post_x, clearance_y, pass_height))
                formations.append("columnn")
                # Nudge the descent point slightly further south (same sign as
                # the crossing) so heading_between(clearance, descent) stays
                # along the crossing direction instead of degenerating to 0,
                # which would swing the column sideways.
                descent_y = clearance_y + cross_direction * 0.1
                if cross_direction < 0:
                    descent_y = max(descent_y, stage_min_y + 0.1)
                else:
                    descent_y = min(descent_y, stage_max_y - 0.1)
                route.append((post_x, descent_y, self.cruise_height))
                formations.append("columnn")

        # No explicit exit waypoint here: the final descent above already
        # parks every drone safely past the last wall at cruise_height, and
        # any retreat toward the stage interior would drag the tail drone
        # back onto (or north of) the wall we just crossed.
        return StagePlan(route, formations)

    def plan_stage3(self) -> StagePlan:
        """Forest traversal using A* + columnn compression.

        Compliant-but-simple strategy that has been verified to fly safely:
          * rally at start_point in ``square``
          * leader follows an A* path through the forest, swarm in ``columnn``
          * one extra ``tail_clear`` waypoint past end_point so the tail drone
            fully exits the last tree before we reform
          * reform into ``square`` at rally_out near end_point
        """
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

        # Drop trailing waypoints whose columnn tail (2 m behind leader) would
        # overlap a tree.
        tree_tail_margin = 0.4
        def _tail_safe(w: Vec3) -> bool:
            tail_x = w[0] - direction * 2.0
            for (tx, ty) in stage.obstacles_global:
                if ((tail_x - tx) ** 2 + (w[1] - ty) ** 2) ** 0.5 < (
                    stage.obstacle_diameter / 2.0 + tree_tail_margin
                ):
                    return False
            return True

        while forest_wps and not _tail_safe(forest_wps[-1]):
            forest_wps.pop()

        rally_in = (start_x, start_y, self.cruise_height)
        tail_clear_x = max(stage_min_x, min(stage_max_x, end_x + direction * 0.6))
        tail_clear = (tail_clear_x, end_y, self.cruise_height)
        rally_out_x = max(stage_min_x, min(stage_max_x, end_x + direction * 0.8))
        rally_out = (rally_out_x, end_y, self.cruise_height)

        # Avoid duplicating start_point between rally_in and the A* path.
        forest_body = (
            forest_wps[1:] if forest_wps and forest_wps[0][:2] == stage.start_point_global else forest_wps
        )
        forest_formations: List[str] = ["columnn" for _ in forest_body]

        waypoints: List[Vec3] = [rally_in] + list(forest_body) + [tail_clear, rally_out]
        formations: List[str] = ["square"] + forest_formations + ["columnn", "square"]
        return StagePlan(waypoints, formations)

    def plan_stage4(self, dynamic_obstacles: Optional[Sequence[DynamicObstacle]] = None) -> StagePlan:
        """Stage4 base plan: straight line from start to end at cruise height.

        Dynamic obstacle avoidance is applied *online* by ``run_stage4`` in
        ``mission_centralised.py`` using real-time topic subscriptions; this
        planner only produces the nominal reference trajectory and default
        formation (``diamond``). The ``dynamic_obstacles`` parameter is kept
        for backwards compatibility / unit tests but is ignored here.
        """
        del dynamic_obstacles  # handled at runtime, not at plan time
        stage = self.scenario.stage4
        if stage is None:
            return StagePlan([], [])
        # Keep the base reference sparse so the reactive loop in
        # ``run_stage4`` can finish each leg quickly (~1 s per waypoint at
        # speed=0.8 m/s). With 9 waypoints over the 8 m path the effective
        # spacing is 1.0 m; we re-sense the obstacle field before each hop.
        base = interpolate_points(
            (stage.start_point_global[0], stage.start_point_global[1], self.cruise_height),
            (stage.end_point_global[0], stage.end_point_global[1], self.cruise_height),
            count=9,
        )
        formations = ["diamond" for _ in base]
        return StagePlan(list(base), formations)

    def plan_all(self, dynamic_obstacles: Optional[Sequence[DynamicObstacle]] = None) -> Dict[str, StagePlan]:
        return {
            "stage1": self.plan_stage1(),
            "stage2": self.plan_stage2(),
            "stage3": self.plan_stage3(),
            "stage4": self.plan_stage4(dynamic_obstacles=dynamic_obstacles),
        }

