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
        # Rally in ``diamond`` at the entry point: this gives us a cheap,
        # safe formation switch (diamond -> columnn) during the long entry
        # segment to the first window's pre-approach, which is the only
        # stretch of open stage we have. Reform between walls is not safe
        # in tight scenarios (see ``if idx_w != last_idx`` branch below).
        formations.append("diamond")

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
                # Hold columnn until the tail drone (~2 m behind the leader)
                # has also cleared the current wall, but never overshoot into
                # the next wall's approach corridor. In tight scenarios (eg.
                # ~4 m between walls) we skip the diamond reform entirely:
                # it would force a lateral spread-and-recompress within less
                # than the column length, which is what caused followers to
                # scrape the wall edges previously.
                post_x, post_y, post_z = transitions[-1]
                next_window = sorted_windows[idx_w + 1]
                next_wy = float(next_window.center_global_xy[1])
                # next_pre lives ``approach_offset`` (=1.1 m) north of the next
                # wall along the travel direction.
                next_pre_y = next_wy - cross_direction * 1.1
                corridor = abs(next_pre_y - post_y)
                # Desired tail-clearance hold is 2.2 m past ``post``, but we
                # leave at least 0.2 m of corridor before next_pre.
                desired_hold = 2.2
                hold_offset = min(desired_hold, max(0.0, corridor - 0.2))
                if hold_offset > 0.05:
                    hold_y = post_y + cross_direction * hold_offset
                    route.append((post_x, hold_y, post_z))
                    formations.append("columnn")
                    hold_anchor_y = hold_y
                else:
                    hold_anchor_y = post_y
                # Diamond reform only if there is >=1.5 m of room after the
                # hold to both spread out and compress back before next_pre.
                remaining = abs(next_pre_y - hold_anchor_y)
                if remaining >= 1.5:
                    reform_offset = min(1.0, remaining - 0.5)
                    reform_y = hold_anchor_y + cross_direction * reform_offset
                    route.append((post_x, reform_y, self.cruise_height))
                    formations.append("diamond")
                # else: stay in columnn straight into next_pre; the scenario
                # is simply too tight for a meaningful diamond reform.
            else:
                # Last window: keep the column at pass_height until the trailing
                # drone has cleared the wall, then descend in-place (same y) so
                # z changes happen without dragging the tail back over the wall.
                post_x, post_y, pass_height = transitions[-1]
                # Push as far past the wall as the stage boundary allows, aiming
                # for 3.5 m past ``post``.
                clearance_y = post_y + cross_direction * 3.5
                if cross_direction < 0:
                    clearance_y = max(clearance_y, stage_min_y + 0.05)
                else:
                    clearance_y = min(clearance_y, stage_max_y - 0.05)
                route.append((post_x, clearance_y, pass_height))
                formations.append("columnn")
                # Compute remaining stage room ahead of clearance; if we are
                # already hugging the boundary, do an in-place descent rather
                # than overshoot and be clamped BACKWARDS (which would reverse
                # the travel direction and confuse the yaw heading).
                if cross_direction < 0:
                    room_ahead = max(0.0, clearance_y - (stage_min_y + 0.1))
                else:
                    room_ahead = max(0.0, (stage_max_y - 0.1) - clearance_y)
                descent_step = min(0.8, room_ahead)
                descent_y = clearance_y + cross_direction * descent_step
                route.append((post_x, descent_y, self.cruise_height))
                formations.append("columnn")

        # No explicit exit waypoint here: the final descent above already
        # parks every drone safely past the last wall at cruise_height, and
        # any retreat toward the stage interior would drag the tail drone
        # back onto (or north of) the wall we just crossed.
        return StagePlan(route, formations)

    def plan_stage3(self) -> StagePlan:
        """Forest traversal: columnn (single-file) from rally to exit.

        Stage3's obstacle field is a dense set of narrow tree trunks. Any
        laterally-wide formation (square, diamond, v, line) risks grazing
        trees with its wingmen even when the leader's path is clear. We
        use ``columnn`` for the ENTIRE stage -- rally, ferry, forest
        traversal, and reform -- so the swarm is always one-drone wide
        and followers literally retrace the leader's A* path via
        trail-following (see ``run_stage3_streaming``).
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

        rally_in = (start_x, start_y, self.cruise_height)
        tail_clear_x = max(stage_min_x, min(stage_max_x, end_x + direction * 0.6))
        tail_clear = (tail_clear_x, end_y, self.cruise_height)

        forest_body = (
            forest_wps[1:] if forest_wps and forest_wps[0][:2] == stage.start_point_global else forest_wps
        )

        waypoints: List[Vec3] = [rally_in] + list(forest_body) + [tail_clear]
        formations: List[str] = ["columnn"] * len(waypoints)
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

