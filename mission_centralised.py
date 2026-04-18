#!/usr/bin/env python3
"""Centralised (leader-follower) mission for COMP0240 CW2."""

from __future__ import annotations

import argparse
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


try:
    import rclpy
    from as2_msgs.msg import BehaviorStatus, YawMode
    from as2_python_api.behavior_actions.behavior_handler import BehaviorHandler
    # Use DroneInterfaceBase (not DroneInterface): avoids eager behaviour servers on init.
    from as2_python_api.drone_interface_base import DroneInterfaceBase as _DroneInterfaceBase
except ImportError:  # Allows dry-run planning without sourcing ROS2 workspace.
    rclpy = None  # type: ignore[assignment]
    BehaviorHandler = object  # type: ignore[assignment]

    class _FallbackBehaviorStatus:
        IDLE = 0

    class _FallbackYawMode:
        PATH_FACING = 0

    class _DroneInterfaceBase:  # type: ignore[override]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

    BehaviorStatus = _FallbackBehaviorStatus  # type: ignore[assignment]
    YawMode = _FallbackYawMode  # type: ignore[assignment]

from centralised.follower_controller import FollowerController
from centralised.leader_planner import LeaderPlanner
from centralised.metrics_export import StageMetrics, export_metrics_to_dir
from centralised.scenario_loader import ScenarioSpec, load_scenario
Vec3 = Tuple[float, float, float]


class MissionDrone(_DroneInterfaceBase):
    """DroneInterfaceBase subclass: lazy behaviours; PositionMotion for streaming."""

    # Short names only: load_module prepends as2_python_api.modules.
    _BEHAVIOUR_MODULES = {
        "takeoff": "takeoff",
        "land": "land",
        "go_to": "go_to",
        "follow_path": "follow_path",
    }

    def __init__(self, namespace: str, verbose: bool = False, use_sim_time: bool = True):
        super().__init__(namespace, verbose=verbose, use_sim_time=use_sim_time)
        self.current_behavior: Optional[BehaviorHandler] = None
        # Populated on-demand by ``ensure_position_motion``. None means the
        # streaming pipeline has not been primed yet.
        self._position_motion = None

    # ------------------------------------------------------------------
    # Lazy behaviour loaders (used by stage1/2/3; stage4 never calls these)
    # ------------------------------------------------------------------
    def _ensure_behavior(self, name: str) -> bool:
        """Load the aerostack2 behaviour module ``name`` if not yet loaded."""
        if hasattr(self, name) and getattr(self, name) is not None:
            return True
        pkg = self._BEHAVIOUR_MODULES.get(name)
        if pkg is None:
            return False
        try:
            self.load_module(pkg)
        except Exception:  # pragma: no cover
            return False
        return hasattr(self, name) and getattr(self, name) is not None

    def ensure_position_motion(self) -> bool:
        """Instantiate the streaming ``PositionMotion`` handler (stage4 core)."""
        if self._position_motion is not None:
            return True
        try:
            from as2_motion_reference_handlers.position_motion import PositionMotion
            self._position_motion = PositionMotion(self)
            return True
        except Exception as exc:
            return False

    def stream_pose(self, pose_xyz: Vec3, yaw_rad: float,
                    twist_limit: Sequence[float] = (1.2, 1.2, 0.6)) -> bool:
        """Publish one position set-point via the streaming handler.

        First call switches the platform into ``POSITION`` control mode
        (3-8 s). Subsequent calls are pure topic publishes.
        """
        if self._position_motion is None:
            if not self.ensure_position_motion():
                return False
        try:
            return self._position_motion.send_position_command_with_yaw_angle(
                pose=[float(pose_xyz[0]), float(pose_xyz[1]), float(pose_xyz[2])],
                twist_limit=list(float(v) for v in twist_limit),
                pose_frame_id="earth",
                twist_frame_id="earth",
                yaw_angle=float(yaw_rad),
            )
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Behaviour-based async helpers (ferry / rally / takeoff-land)
    # ------------------------------------------------------------------
    def go_to_async(self, target: Vec3, speed: float, yaw_mode: int, yaw_angle: Optional[float] = None) -> None:
        if not self._ensure_behavior("go_to"):
            return
        self.current_behavior = self.go_to  # type: ignore[attr-defined]
        self.current_behavior(
            target[0], target[1], target[2],
            speed, yaw_mode, yaw_angle, "earth", False,
        )

    def takeoff_async(self, height: float, speed: float) -> None:
        if not self._ensure_behavior("takeoff"):
            return
        self.current_behavior = self.takeoff  # type: ignore[attr-defined]
        self.current_behavior(height, speed, False)

    def land_async(self, speed: float) -> None:
        if not self._ensure_behavior("land"):
            return
        self.current_behavior = self.land  # type: ignore[attr-defined]
        self.current_behavior(speed, False)

    def reached_goal(self) -> bool:
        if not self.current_behavior:
            return False
        return self.current_behavior.status == BehaviorStatus.IDLE


class CentralisedMission:
    """Coordinates leader-follower execution across all challenge stages."""

    def __init__(
        self,
        scenario: ScenarioSpec,
        namespaces: Sequence[str],
        verbose: bool = False,
        use_sim_time: bool = True,
        dry_run: bool = False,
        behavior_timeout_s: float = 120.0,
    ) -> None:
        self.scenario = scenario
        self.namespaces = list(namespaces)
        self.verbose = verbose
        self.use_sim_time = use_sim_time
        self.dry_run = dry_run
        self.behavior_timeout_s = behavior_timeout_s
        self.planner = LeaderPlanner(scenario=scenario, cruise_height=1.2)
        self.controller = FollowerController(follower_count=max(0, len(self.namespaces) - 1))
        self.metrics: Dict[str, StageMetrics] = {}

        # Gazebo spawn xyz from world YAML (stage4 default world path).
        self._world_gazebo_spawn_xyz: Dict[str, Tuple[float, float, float]] = {}
        if scenario.stage4 is not None:
            _wp = Path(__file__).resolve().parent / "config_sim" / "config" / "world_swarm_stage4.yaml"
            if _wp.is_file():
                try:
                    from centralised.scenario_loader import load_world_drone_spawns_by_model_name

                    self._world_gazebo_spawn_xyz = load_world_drone_spawns_by_model_name(_wp)
                except Exception:
                    pass

        self.drones: List[MissionDrone] = []
        if not dry_run:
            # Parallel construction: DroneInterfaceBase.__init__ blocks on a
            # 0.5 s sleep + DDS discovery. Serial creation of 5 drones took
            # the majority of mission startup; with a 5-thread pool we pay
            # that cost once instead of 5x.
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max(1, len(self.namespaces))) as ex:
                futs = {
                    ns: ex.submit(MissionDrone, ns, verbose, use_sim_time)
                    for ns in self.namespaces
                }
                self.drones = [futs[ns].result() for ns in self.namespaces]
            
        # Stage4 real-time dynamic obstacle sensing.
        self.obstacle_monitor = None
        if not dry_run and self.scenario.stage4 is not None and rclpy is not None:
            try:
                from centralised.dynamic_obstacle_monitor import DynamicObstacleMonitor

                self.obstacle_monitor = DynamicObstacleMonitor(
                    obstacle_diameter=self.scenario.stage4.obstacle_diameter,
                )
                self.obstacle_monitor.start()
            except Exception:  # pragma: no cover - fall back to static plan
                self.obstacle_monitor = None

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message)

    def _wait_all(self, context: str = "") -> bool:
        if self.dry_run:
            return True
        timeout_s = self.behavior_timeout_s
        start = time.time()
        while time.time() - start < timeout_s:
            all_finished = True
            for drone in self.drones:
                all_finished = all_finished and drone.reached_goal()
            if all_finished:
                return True
            time.sleep(0.05)
        self._log(f"[_wait_all] timeout after {timeout_s}s context={context!r}")
        return False

    def arm_and_offboard(self) -> bool:
        """Arm + offboard in PARALLEL across all drones.

        Previous version called ``drone.arm()`` + ``drone.offboard()``
        serially. ``Arm.__init__`` does ``wait_for_service`` with a 45 s
        default timeout; multiply by 5 drones x 2 services and we could
        stall 7+ minutes. Running in a 5-thread pool means all 5 drones
        hit the service discovery simultaneously, and if the services are
        up we return in the time of the slowest drone (typically 2-5 s).
        """
        if self.dry_run:
            return True
        from concurrent.futures import ThreadPoolExecutor

        def _arm_one(d: MissionDrone) -> Tuple[str, bool, bool]:
            arm_ok = False
            for attempt in range(3):
                try:
                    arm_ok = bool(d.arm())
                    if arm_ok:
                        break
                except Exception:
                    time.sleep(0.4 * (attempt + 1))
            off_ok = False
            for attempt in range(3):
                try:
                    off_ok = bool(d.offboard())
                    if off_ok:
                        break
                except Exception:
                    time.sleep(0.4 * (attempt + 1))
            return (d.namespace, bool(arm_ok), bool(off_ok))

        with ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            results = list(ex.map(_arm_one, self.drones))
        ok = all(a and o for _, a, o in results)
        return ok

    def takeoff(self, height: float = 1.2, speed: float = 0.8) -> bool:
        if self.dry_run:
            return True
        for drone in self.drones:
            drone.takeoff_async(height=height, speed=speed)
        return self._wait_all(context="takeoff")

    def land(self, speed: float = 0.6) -> bool:
        if self.dry_run:
            return True
        for drone in self.drones:
            drone.land_async(speed=speed)
        return self._wait_all(context="land")

    # ------------------------------------------------------------------
    # Streaming takeoff / land (no behaviour action; stage4 fast path)
    # ------------------------------------------------------------------
    def streaming_takeoff(self, target_z: float = 1.2, climb_rate: float = 0.5,
                          yaw_rad: float = 0.0, settle_s: float = 2.0) -> bool:
        """Takeoff by ramping a position set-point at 20 Hz.

        Bypasses TakeoffBehavior entirely. The first set-point per drone
        triggers a control-mode switch into POSITION (~3-8 s); subsequent
        set-points are millisecond-latency topic publishes.

        :param target_z: cruise altitude [m]
        :param climb_rate: vertical velocity limit [m/s]
        :param yaw_rad: yaw angle for the climb [rad]
        :param settle_s: hover at target_z for this long before returning
        :returns: True iff every drone reached within 0.4 m of target_z
        """
        if self.dry_run:
            return True
        import concurrent.futures as cf

        def _earth_xyz(d: "MissionDrone") -> Tuple[float, float, float]:
            try:
                px, py, pz = float(d.position[0]), float(d.position[1]), float(d.position[2])
                if math.isfinite(px) and math.isfinite(py) and math.isfinite(pz):
                    return (px, py, pz)
            except Exception:
                pass
            if self.scenario.stage4 is not None:
                fb = self._world_gazebo_spawn_xyz.get(d.namespace)
                if fb is not None:
                    return fb
            return (0.0, 0.0, 0.0)

        # 1. Parallel priming of PositionMotion on each drone.
        spawn_xy: List[Tuple[float, float, float]] = [_earth_xyz(d) for d in self.drones]

        def _prime(args):
            d, (x, y, z) = args
            ok = d.stream_pose((x, y, max(z, 0.1)), yaw_rad, [0.6, 0.6, climb_rate])
            return (d.namespace, bool(ok))

        with cf.ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            primed = list(ex.map(_prime, zip(self.drones, spawn_xy)))
        if not all(ok for _, ok in primed):
            return False

        # 1b. Brief wait for POSITION mode on all drones (control switch is async).
        t_deck = time.time()
        deck_z = 0.22
        while time.time() - t_deck < 12.0:
            try:
                if all(float(d.position[2]) > deck_z for d in self.drones):
                    break
            except Exception:
                pass
            time.sleep(0.15)

        # 2. Ramp z from each drone's spawn altitude to target_z.
        start_z = max(0.0, min(s[2] for s in spawn_xy))
        distance = max(0.1, target_z - start_z)
        ramp_duration = max(2.0, distance / max(0.1, climb_rate) + 1.0)
        rate_hz = 20.0
        dt = 1.0 / rate_hz

        t0 = time.time()
        while True:
            elapsed = time.time() - t0
            progress = min(1.0, elapsed / ramp_duration)
            z_cmd = start_z + (target_z - start_z) * progress
            for (d, (sx, sy, _)) in zip(self.drones, spawn_xy):
                d.stream_pose((sx, sy, z_cmd), yaw_rad, [0.6, 0.6, climb_rate])
            if elapsed >= ramp_duration:
                break
            time.sleep(dt)

        # 3. Hover at target_z for settle_s so the platform stabilises.
        t0 = time.time()
        while time.time() - t0 < settle_s:
            for (d, (sx, sy, _)) in zip(self.drones, spawn_xy):
                d.stream_pose((sx, sy, target_z), yaw_rad, [0.4, 0.4, 0.4])
            time.sleep(dt)

        # 4. Altitude check (all within 0.4 m of target_z).
        for d in self.drones:
            try:
                if abs(float(d.position[2]) - target_z) > 0.4:
                    return False
            except Exception:
                return False
        return True

    def streaming_land(self, yaw_rad: float = 0.0, descent_rate: float = 0.4) -> bool:
        """Gentle descent via streaming set-point (no LandBehavior)."""
        if self.dry_run:
            return True
        rate_hz = 20.0
        dt = 1.0 / rate_hz
        # Freeze xy at current position for each drone; ramp z toward 0.
        fixed = []
        for d in self.drones:
            try:
                fixed.append((float(d.position[0]), float(d.position[1]), float(d.position[2])))
            except Exception:
                fixed.append((0.0, 0.0, 1.0))
        max_z = max(f[2] for f in fixed)
        duration = max(2.0, max_z / max(0.1, descent_rate) + 1.5)
        t0 = time.time()
        while True:
            elapsed = time.time() - t0
            progress = min(1.0, elapsed / duration)
            for (d, (fx, fy, fz)) in zip(self.drones, fixed):
                z_cmd = max(0.05, fz * (1.0 - progress))
                d.stream_pose((fx, fy, z_cmd), yaw_rad, [0.3, 0.3, descent_rate])
            if elapsed >= duration:
                break
            time.sleep(dt)
        
        self._reset_platform_state(source="streaming_land")
        return True

    def _ensure_motion_ref_loaded(self) -> bool:
        """Attach ``PositionMotion`` on every drone (streaming stages)."""
        if self.dry_run:
            return True
        return all(d.ensure_position_motion() for d in self.drones)

    def _stream_position(
        self,
        drone: MissionDrone,
        pose_xyz: Vec3,
        yaw_rad: float,
        twist_limit: Sequence[float],
    ) -> bool:
        if self.dry_run:
            return True
        return drone.stream_pose(pose_xyz, yaw_rad, twist_limit)

    def _reset_platform_state(self, source: str = "unknown") -> None:
        """Drop offboard and disarm on every drone so a subsequent mission
        run (against the SAME sim) can cleanly re-acquire the platform.

        Without this the platform is left at (armed=True, offboard=True)
        holding the last streamed set-point. The NEXT mission script then
        hangs inside arm()/offboard() because the as2 platform services
        don't re-answer when the platform is already in the requested
        state. We call manual() + disarm() in parallel with a short
        timeout so one unresponsive drone can't stall teardown.
        """
        if self.dry_run:
            return
        self._log(f"[_reset_platform_state] source={source}")
        from concurrent.futures import ThreadPoolExecutor

        def _reset_one(d):
            for attempt in range(3):
                try:
                    if bool(d.manual()):
                        break
                except Exception:
                    pass
                time.sleep(0.3)
            for attempt in range(3):
                try:
                    if bool(d.disarm()):
                        break
                except Exception:
                    pass
                time.sleep(0.3)

        with ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            futs = [ex.submit(_reset_one, d) for d in self.drones]
            for f in futs:
                try:
                    f.result(timeout=5.0)
                except Exception:
                    pass

    def shutdown(self) -> None:
        if self.obstacle_monitor is not None:
            try:
                self.obstacle_monitor.shutdown()
            except Exception:
                pass
        for drone in self.drones:
            drone.shutdown()

    def _command_swarm_step(
        self,
        leader_target: Vec3,
        follower_targets: Sequence[Vec3],
        speed: float,
        yaw_mode: int,
        yaw_angle: Optional[float] = None,
        wait_for_completion: bool = True,
    ) -> bool:
        if self.dry_run:
            return True
        self.drones[0].go_to_async(leader_target, speed=speed, yaw_mode=yaw_mode, yaw_angle=yaw_angle)
        for idx, target in enumerate(follower_targets, start=1):
            self.drones[idx].go_to_async(target, speed=speed, yaw_mode=yaw_mode, yaw_angle=yaw_angle)
        if not wait_for_completion:
            return True
        return self._wait_all(context="go_to_step")

    def run_stage1(self) -> StageMetrics:
        """Stage1: streaming-only (10 Hz ``PositionMotion`` circle cruise)."""
        from centralised.stages.stage1_runner import run_stage1_streaming_impl

        return run_stage1_streaming_impl(self)

    def run_stage2(self) -> StageMetrics:
        """Stage2: streaming-only (10 Hz polyline through windows)."""
        from centralised.stages.stage2_runner import run_stage2_streaming_impl

        return run_stage2_streaming_impl(self)

    def run_stage3(self) -> StageMetrics:
        """Stage3: streaming-only (10 Hz forest polyline)."""
        from centralised.stages.stage3_runner import run_stage3_streaming_impl

        return run_stage3_streaming_impl(self)

    def run_stage4(self) -> StageMetrics:
        """Stage4: dynamic obstacles + 10 Hz streaming cruise (see ``stage4_runner``)."""
        from centralised.stages.stage4_runner import run_stage4_impl

        return run_stage4_impl(self)

    def _stage_entry_xy(self, name: str) -> Optional[Tuple[float, float]]:
        """Return the (x, y) at which ``name`` expects to start in stage1-3.

        Stage 4 streams its own takeoff and entry, so we still ferry the swarm
        to stage4's start_point so it doesn't have to fly across stage3's
        forest at low altitude during its own warmup.
        """
        s = self.scenario
        if name == "stage1" and s.stage1 is not None:
            wps = self.planner.plan_stage1().waypoints
            if wps:
                return (wps[0][0], wps[0][1])
        if name == "stage2" and s.stage2 is not None:
            wps = self.planner.plan_stage2().waypoints
            if wps:
                return (wps[0][0], wps[0][1])
        if name == "stage3" and s.stage3 is not None:
            return (s.stage3.start_point_global[0], s.stage3.start_point_global[1])
        if name == "stage4" and s.stage4 is not None:
            return (s.stage4.start_point_global[0], s.stage4.start_point_global[1])
        return None

    def _stage_exit_xy(self, name: str) -> Optional[Tuple[float, float]]:
        """Best-effort exit pose of a stage (used as ferry origin for the next)."""
        s = self.scenario
        if name == "stage1" and s.stage1 is not None:
            wps = self.planner.plan_stage1().waypoints
            if wps:
                return (wps[-1][0], wps[-1][1])
        if name == "stage2" and s.stage2 is not None:
            wps = self.planner.plan_stage2().waypoints
            if wps:
                return (wps[-1][0], wps[-1][1])
        if name == "stage3" and s.stage3 is not None:
            wps = self.planner.plan_stage3().waypoints
            if wps:
                return (wps[-1][0], wps[-1][1])
        if name == "stage4" and s.stage4 is not None:
            return (s.stage4.end_point_global[0], s.stage4.end_point_global[1])
        return None

    def _inter_stage_ferry(
        self,
        from_xy: Tuple[float, float],
        to_xy: Tuple[float, float],
        ferry_height: float = 6.0,
        ferry_speed: float = 1.2,
    ) -> bool:
        """Move swarm via high-altitude legs (above obstacles) in line formation."""
        if self.dry_run:
            return True
        cx, cy = from_xy
        tx, ty = to_xy
        cruise_h = self.planner.cruise_height
        legs: List[Vec3] = [
            (cx, cy, ferry_height),
            (tx, ty, ferry_height),
            (tx, ty, cruise_h),
        ]
        prev: Tuple[float, float] = from_xy
        for waypoint in legs:
            dx = waypoint[0] - prev[0]
            dy = waypoint[1] - prev[1]
            yaw = math.atan2(dy, dx) if math.hypot(dx, dy) > 1e-3 else 0.0
            follower_targets = self.controller.compute_targets(
                leader_position=waypoint,
                leader_yaw=yaw,
                formation_name="line",
                previous_formation_name="line",
                transition_alpha=1.0,
            )
            ok = self._command_swarm_step(
                leader_target=waypoint,
                follower_targets=follower_targets,
                speed=ferry_speed,
                yaw_mode=YawMode.PATH_FACING,
                yaw_angle=None,
            )
            if not ok:
                self._log(f"[ferry] command timeout flying to {waypoint}")
                return False
            prev = (waypoint[0], waypoint[1])
        return True

    def run_selected_stages(self, stage: str) -> Dict[str, StageMetrics]:
        ordered = [("stage1", self.run_stage1), ("stage2", self.run_stage2), ("stage3", self.run_stage3), ("stage4", self.run_stage4)]
        outputs: Dict[str, StageMetrics] = {}
        last_exit: Optional[Tuple[float, float]] = None
        for name, fn in ordered:
            if stage != "all" and stage != name:
                continue
            if stage == "all" and last_exit is not None:
                entry = self._stage_entry_xy(name)
                if entry is not None:
                    self._log(f"Inter-stage ferry: {last_exit} -> {entry} (above obstacles)")
                    self._inter_stage_ferry(from_xy=last_exit, to_xy=entry)
            self._log(f"Running {name}")
            outputs[name] = fn()
            exit_xy = self._stage_exit_xy(name)
            if exit_xy is not None:
                last_exit = exit_xy
        return outputs

    def export_metrics(self, output_dir: Path) -> None:
        """Write ``阶段N_仿真结果.md`` per stage (see ``centralised.metrics_export``)."""
        export_metrics_to_dir(
            self.metrics,
            output_dir,
            self.scenario,
            getattr(self, "scenario_path", "") or "",
            len(self.drones) if getattr(self, "drones", None) else 5,
            self._log,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Centralised leader-follower mission for CW2")
    parser.add_argument("--scenario", type=str, default="scenarios/scenario1.yaml", help="Scenario YAML path")
    parser.add_argument(
        "--stage",
        type=str,
        default="all",
        choices=["all", "stage1", "stage2", "stage3", "stage4"],
        help="Run only one stage or all stages",
    )
    parser.add_argument(
        "-n",
        "--namespaces",
        nargs="+",
        default=["drone0", "drone1", "drone2", "drone3", "drone4"],
        help="Drone namespaces (leader is first item)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logs")
    parser.add_argument("-s", "--use-sim-time", action="store_true", default=True, help="Use simulation time")
    parser.add_argument("--dry-run", action="store_true", help="Skip ROS execution, only compute plans and metrics")
    parser.add_argument(
        "--metrics-dir",
        type=str,
        default="results/centralised",
        help="Output directory for per-stage Markdown metrics (阶段N_仿真结果.md)",
    )
    parser.add_argument(
        "--behavior-timeout",
        type=float,
        default=120.0,
        help="Timeout in seconds for each coordinated behavior step",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenario = load_scenario(args.scenario)

    if args.dry_run:
        mission = CentralisedMission(
            scenario=scenario,
            namespaces=args.namespaces,
            verbose=args.verbose,
            use_sim_time=args.use_sim_time,
            dry_run=True,
            behavior_timeout_s=args.behavior_timeout,
        )
        mission.scenario_path = args.scenario
        mission.run_selected_stages(args.stage)
        mission.export_metrics(Path(args.metrics_dir))
        print(f"Dry run done. Metrics written to: {args.metrics_dir}")
        return 0

    if rclpy is None:
        print("ROS2 environment not sourced. Re-run with --dry-run or source install/setup.bash.")
        return 2

    rclpy.init()
    mission = CentralisedMission(
        scenario=scenario,
        namespaces=args.namespaces,
        verbose=args.verbose,
        use_sim_time=args.use_sim_time,
        dry_run=False,
        behavior_timeout_s=args.behavior_timeout,
    )
    mission.scenario_path = args.scenario
    # Stage4: streaming takeoff/land. Other stages use behaviour takeoff/land.
    use_streaming_flight = (args.stage == "stage4")

    try:
        if not mission.arm_and_offboard():
            print("Arm/offboard failed.")
            return 1

        if use_streaming_flight:
            # Cruise height is 1.8 m (see run_stage4). Taking off directly
            # to 1.8 m skips a post-takeoff climb phase during which
            # drones in diamond would still be in ground-effect/downwash.
            if not mission.streaming_takeoff(target_z=1.8, climb_rate=0.6):
                print("Streaming takeoff failed (altitude not reached).")
                # Continue anyway: the cruise loop will keep streaming set-
                # points and may still recover. Only abort if NOTHING is in
                # the air.
                any_up = any(float(d.position[2]) > 0.5 for d in mission.drones)
                if not any_up:
                    return 1
        else:
            if not mission.takeoff():
                print("Takeoff timeout.")
                return 1

        mission.run_selected_stages(args.stage)

        if use_streaming_flight:
            mission.streaming_land()
        else:
            mission.land()
        mission.export_metrics(Path(args.metrics_dir))
        return 0
    finally:
        # Guarantee platform state is reset before process exit, even on
        # Ctrl-C / crash mid-flight. Without this, a mid-flight abort leaves
        # the drones at (armed, offboard, last-setpoint) and the next
        # mission script hangs inside arm()/offboard(). Idempotent w.r.t.
        # streaming_land's own reset call.
        try:
            mission._reset_platform_state(source="main.finally")
        except Exception:
            pass
        try:
            mission.shutdown()
        except Exception:
            pass
        rclpy.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

