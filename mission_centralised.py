#!/usr/bin/env python3
"""Centralised (leader-follower) mission for COMP0240 CW2."""

from __future__ import annotations

import argparse
import csv
import json as _json
import math
import os as _os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# #region agent log
_DBG_LOG_PATH = "/home/tianhaoliang/.cursor/debug.log"


def _dbg(location: str, message: str, data: Optional[Dict[str, Any]] = None, hypothesis_id: str = "", run_id: str = "run1") -> None:
    try:
        entry = {
            "id": f"log_{int(time.time()*1000)}_{_os.getpid()}",
            "timestamp": int(time.time() * 1000),
            "location": location,
            "message": message,
            "data": data or {},
            "runId": run_id,
            "hypothesisId": hypothesis_id,
        }
        with open(_DBG_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(_json.dumps(entry) + "\n")
    except Exception:
        pass
# #endregion

try:
    import rclpy
    from as2_msgs.msg import BehaviorStatus, YawMode
    from as2_python_api.behavior_actions.behavior_handler import BehaviorHandler
    # IMPORTANT: We subclass DroneInterfaceBase (NOT DroneInterface). The
    # difference is that DroneInterface eagerly instantiates four behaviour
    # modules (Takeoff/GoTo/FollowPath/Land) in __init__, each of which does
    # 4x wait_for_server with a 60 s timeout (see
    # as2_python_api.behavior_actions.behavior_handler.BehaviorHandler.TIMEOUT).
    # With 5 drones the cold-discovery storm easily burns several minutes,
    # and after it finishes the *first* send_goal on each behaviour still
    # takes ~75 s to be accepted. Runtime evidence in previous debug.log:
    # drone0 takeoff_async alone took 75 s to return; rally dispatch across
    # 5 drones took 110 s, during which drones had no active set-point and
    # free-fell in Gazebo (drone1/drone4 ended on the ground at z<0.05 m).
    #
    # DroneInterfaceBase exposes everything we need (.position, .info, .arm,
    # .offboard, .shutdown) without loading any behaviour. Stage1/2/3 paths
    # that still call drone.takeoff_async/go_to_async/land_async will lazily
    # load the corresponding module on first use; stage4 never loads any
    # behaviour and drives the platform exclusively via the
    # motion_reference_handler streaming topic (pure publisher, no action
    # round-trip, first setpoint takes ~3-8 s to switch control mode, after
    # that set-points are millisecond-latency).
    from as2_python_api.drone_interface_base import DroneInterfaceBase as _DroneInterfaceBase
except ImportError:  # Allows dry-run planning without sourcing ROS2 workspace.
    rclpy = None  # type: ignore[assignment]
    BehaviorHandler = object  # type: ignore[assignment]

    class _FallbackBehaviorStatus:
        IDLE = 0

    class _FallbackYawMode:
        PATH_FACING = 0

    class _DroneInterfaceBase:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            pass

    BehaviorStatus = _FallbackBehaviorStatus  # type: ignore[assignment]
    YawMode = _FallbackYawMode  # type: ignore[assignment]

from centralised.follower_controller import FollowerController
from centralised.leader_planner import LeaderPlanner, StagePlan
from centralised.scenario_loader import ScenarioSpec, load_scenario
from centralised.utils import heading_between


Vec3 = Tuple[float, float, float]


@dataclass
class StageMetrics:
    """Per-stage metrics container.

    Field groups
    ------------
    The README (and the CW2 brief referenced from it) lists FIVE evaluation
    axes for the coursework -- see ``README.md`` lines 67 and 79:

        * Success rate
        * Reconfigurability (i.e. can the swarm actually change formation?)
        * Time taken
        * Collision avoidance
        * Efficiency in movement

    All five are populated as *official metrics* below so the CSV we emit
    can be read straight across to a decentralised run for comparison.

    Additional centralised-specific *analytical* fields
    (``formation_rmse_mean``, ``completion_ratio``) are kept for detail but
    are NOT used for pass/fail scoring.

    Legacy fields (``completed_waypoints``, ``planned_waypoints``,
    ``collisions``) are retained so stage2/3/4 code paths that still write
    them keep working; they are mapped into the official fields at export
    time.
    """

    stage: str

    # --- Legacy / internal (kept for backward compatibility with
    # --- stage2/3/4 behaviour-based code paths).
    completed_waypoints: int = 0
    planned_waypoints: int = 0
    collisions: int = 0
    start_time: float = 0.0
    end_time: float = 0.0

    # === OFFICIAL metric axis 1: Success rate =================================
    # Pass/fail outcome of this stage, judged against a stage-specific rule
    # (see ``success_criterion``). ``None`` means "not evaluated yet".
    success: Optional[bool] = None
    success_criterion: str = ""  # human-readable description of the rule

    # === OFFICIAL metric axis 2: Reconfigurability ============================
    # How many distinct formations were actually shown vs planned.
    formations_planned: int = 0
    formations_visited: int = 0

    # === OFFICIAL metric axis 3: Time taken ===================================
    # Exposed via ``duration_sec`` property below.

    # === OFFICIAL metric axis 4: Collision avoidance ==========================
    # Tick count during which ANY pair of drones was closer than the
    # 0.30 m "near-miss warning" threshold. This is the conservative
    # (strict) threshold used by default success judgement.
    num_pair_collision_ticks: int = 0
    # Tick count during which ANY pair of drones got closer than the
    # 0.15 m "physical body + propeller" radius sum. Anything below this
    # is an actual physical hit (not just a near miss).
    num_pair_ticks_physical: int = 0
    # Worst-case minimum pairwise XY distance across the whole swarm.
    min_pair_distance_m: Optional[float] = None
    # Tick count during which any drone got within the safety buffer of a
    # static/dynamic obstacle (stage2/3/4 only; None on stages with no
    # obstacles). ``_collision_ticks`` is the legacy name and corresponds
    # to the safety buffer; ``_physical`` is the physical-contact count.
    num_obstacle_collision_ticks: Optional[int] = None
    num_obstacle_ticks_physical: Optional[int] = None

    # === OFFICIAL metric axis 5: Efficiency in movement =======================
    # Actual path length flown by the leader (m).
    leader_path_length_m: Optional[float] = None
    # Ideal path length given the mission geometry (stage-specific):
    #   stage1 : circumference * num_loops    (closed ring)
    #   stage2 : sum of windowed sub-legs     (corridor)
    #   stage3 : Euclidean(start, end)        (or A* length if known)
    #   stage4 : Euclidean(start, end)        (straight cruise leg)
    ideal_path_length_m: Optional[float] = None

    # === Centralised-specific analytical extras (not used for pass/fail) =====
    # RMS of (actual follower position - commanded follower slot target).
    formation_rmse_mean: Optional[float] = None
    # Fraction of the nominal circular route the leader actually flew
    # (leader's angular travel / planned angular travel). Stage1 only.
    completion_ratio: Optional[float] = None
    # Number of formation transitions successfully commanded.
    formation_switches: Optional[int] = None

    # === Time-taken breakdown (what the brief actually asks to report) =======
    # Per-formation display duration in seconds (e.g. {"line": 15.71, ...}).
    # Populated by each run_stageN_streaming.
    time_per_formation: Dict[str, float] = field(default_factory=dict)
    # Stage1-only: time to complete ONE full lap of the circular trajectory.
    time_per_lap_s: Optional[float] = None
    # Stage1-only: total number of laps flown during cruise.
    laps_completed: Optional[float] = None
    # Cruise time excluding rally / ferry / hover phases.
    cruise_time_s: Optional[float] = None

    # === Stage-specific fairness fields ======================================
    # Stage2: how many of the planned windows were actually traversed.
    windows_planned: Optional[int] = None
    windows_passed: Optional[int] = None
    # Stage3/4: detour ratio = leader_path_length_m / euclid(start, end).
    # 1.0 = flew in a straight line; larger = more obstacle avoidance detour.
    detour_ratio: Optional[float] = None
    # Stage4: min observed distance to any dynamic obstacle.
    min_dynamic_obstacle_distance_m: Optional[float] = None
    # Stage4: number of distinct close encounters with dynamic obstacles.
    dynamic_obstacle_encounters: Optional[int] = None

    # === Dual-threshold success (fair comparison across methods) =============
    # "Strict" success: uses 0.30 m near-miss threshold (default, conservative).
    # "Physical" success: uses 0.15 m physical-contact threshold (lenient).
    # Both are reported so a decentralised run can be compared under either.
    success_strict: Optional[bool] = None
    success_physical: Optional[bool] = None

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.end_time - self.start_time)

    @property
    def reconfigurability(self) -> Optional[float]:
        """Fraction of planned formations the run actually visited, in [0,1]."""
        if self.formations_planned <= 0:
            return None
        return min(1.0, self.formations_visited / float(self.formations_planned))

    @property
    def path_efficiency(self) -> Optional[float]:
        """Ratio min(ideal, actual) / max(ideal, actual), in (0, 1].

        Symmetric form so that BOTH "flew too far" (detours) AND "flew too
        little" (didn't finish) count as low efficiency. 1.0 means the
        leader flew exactly the ideal distance.
        """
        if (self.ideal_path_length_m is None or self.leader_path_length_m is None
                or self.ideal_path_length_m <= 0.0 or self.leader_path_length_m <= 0.0):
            return None
        ideal = float(self.ideal_path_length_m)
        actual = float(self.leader_path_length_m)
        return min(ideal, actual) / max(ideal, actual)

    @property
    def success_rate(self) -> float:
        """Legacy numeric form of ``success`` (1.0/0.0).

        Older plotting code still pulls this. Prefer ``success`` (bool)
        and the explicit ``success_criterion`` string for reports.
        """
        if self.success is not None:
            return 1.0 if self.success else 0.0
        # Fallback for stages that haven't set ``success`` yet: use the old
        # tick-ratio definition so pre-existing behaviour is unchanged.
        if self.planned_waypoints <= 0:
            return 0.0
        return min(1.0, self.completed_waypoints / float(self.planned_waypoints))


def _greedy_slot_assignment(
    current_positions: Sequence[Tuple[float, float]],
    target_slots: Sequence[Tuple[float, float]],
) -> List[int]:
    """Greedy nearest-neighbour assignment (slot -> drone index).

    For each slot (in order), picks the unassigned drone with the smallest
    squared XY distance. Returns a list ``slot_to_drone`` where
    ``slot_to_drone[slot_i]`` is the index into ``current_positions``.
    This prevents the crossing paths we saw when followers were mapped by
    ROS namespace order rather than physical proximity -- e.g. drone3 at
    (0, +1) being sent to a slot on the -y side of the leader while
    drone4 at (0, -1) was sent to the +y side, which made them cross
    right through each other on takeoff.
    """
    n_slots = len(target_slots)
    n_drones = len(current_positions)
    slot_to_drone: List[int] = [-1] * n_slots
    assigned: set = set()
    for slot_idx in range(min(n_slots, n_drones)):
        sx, sy = target_slots[slot_idx]
        best_d = float("inf")
        best_i = -1
        for di, (px, py) in enumerate(current_positions):
            if di in assigned:
                continue
            d2 = (sx - px) * (sx - px) + (sy - py) * (sy - py)
            if d2 < best_d:
                best_d = d2
                best_i = di
        if best_i >= 0:
            slot_to_drone[slot_idx] = best_i
            assigned.add(best_i)
    return slot_to_drone


class MissionDrone(_DroneInterfaceBase):
    """Minimal drone wrapper. Behaviours loaded lazily, NOT in __init__.

    By subclassing ``DroneInterfaceBase`` (not ``DroneInterface``) we avoid
    the 4x ``BehaviorHandler.__init__`` calls that ``DroneInterface`` does
    eagerly, each of which worst-case waits 240 s for action+service
    servers. For stage4 (streaming-only) no behaviour is ever loaded. For
    stage1/2/3 (still behaviour-based), ``_ensure_behavior`` lazily loads
    takeoff/go_to/land on first use.
    """

    # Module alias -> module name consumed by DroneInterfaceBase.load_module.
    #
    # IMPORTANT: load_module() already resolves inside
    # ``as2_python_api.modules.<name>`` (or ``<name>_module``). Passing a full
    # absolute import path here makes it prepend ``as2_python_api.modules.``
    # again, which becomes the broken
    # ``as2_python_api.modules.as2_python_api.modules.takeoff_module`` and
    # silently disables takeoff/go_to/land for stage1-3/all.
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
        except Exception as exc:  # pragma: no cover
            _dbg(
                "mission_centralised.py:_ensure_behavior",
                f"load_module failed for {name}",
                {"ns": self.namespace, "pkg": pkg, "error": repr(exc)},
                hypothesis_id="H9",
            )
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
            _dbg(
                "mission_centralised.py:ensure_position_motion",
                "PositionMotion init failed",
                {"ns": self.namespace, "error": repr(exc)},
                hypothesis_id="H9",
            )
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
        except Exception as exc:
            _dbg(
                "mission_centralised.py:stream_pose",
                "send_position_command failed",
                {"ns": self.namespace, "error": repr(exc)},
                hypothesis_id="H9",
            )
            return False

    # ------------------------------------------------------------------
    # Behaviour-based async helpers (stage1/2/3 legacy)
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
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=max(1, len(self.namespaces))) as ex:
                futs = {
                    ns: ex.submit(MissionDrone, ns, verbose, use_sim_time)
                    for ns in self.namespaces
                }
                self.drones = [futs[ns].result() for ns in self.namespaces]
            _dbg(
                "mission_centralised.py:__init__",
                "drones constructed in parallel",
                {"count": len(self.drones), "elapsed_s": round(time.time() - t0, 2),
                 "ns": [d.namespace for d in self.drones]},
                hypothesis_id="H9",
            )

        # Stage4 real-time dynamic obstacle sensing.
        self.obstacle_monitor = None
        if not dry_run and self.scenario.stage4 is not None and rclpy is not None:
            try:
                from centralised.dynamic_obstacle_monitor import DynamicObstacleMonitor

                self.obstacle_monitor = DynamicObstacleMonitor(
                    obstacle_diameter=self.scenario.stage4.obstacle_diameter,
                )
                self.obstacle_monitor.start()
                _dbg(
                    "mission_centralised.py:__init__",
                    "dynamic obstacle monitor started",
                    {"topic": "/dynamic_obstacles/locations", "radius": self.scenario.stage4.obstacle_diameter / 2.0},
                    hypothesis_id="H6",
                )
            except Exception as exc:  # pragma: no cover - fall back to static plan
                self.obstacle_monitor = None
                _dbg(
                    "mission_centralised.py:__init__",
                    "dynamic obstacle monitor failed to start",
                    {"error": repr(exc)},
                    hypothesis_id="H6",
                )

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message)

    def _wait_all(self, context: str = "") -> bool:
        if self.dry_run:
            return True
        timeout_s = self.behavior_timeout_s
        start = time.time()
        last_log = 0.0
        while time.time() - start < timeout_s:
            all_finished = True
            for drone in self.drones:
                all_finished = all_finished and drone.reached_goal()
            if all_finished:
                # #region agent log
                _dbg(
                    "mission_centralised.py:_wait_all",
                    f"all drones reached goal ({context})",
                    {"elapsed_s": round(time.time() - start, 2), "context": context},
                    hypothesis_id="H1",
                )
                # #endregion
                return True
            now = time.time()
            if now - last_log >= 3.0:
                # #region agent log
                statuses = []
                for d in self.drones:
                    beh = d.current_behavior
                    st = None
                    try:
                        st = int(beh.status) if beh is not None else None
                    except Exception:
                        st = "err"
                    try:
                        pos = list(getattr(d, "position", [None, None, None]))
                    except Exception:
                        pos = [None, None, None]
                    try:
                        info = d.info
                        info_min = {k: info.get(k) for k in ("armed", "offboard", "state")}
                    except Exception:
                        info_min = {}
                    statuses.append({
                        "ns": d.namespace,
                        "beh_status": st,
                        "pos": pos,
                        "info": info_min,
                    })
                # #region agent log
                mem_info = {}
                try:
                    with open("/proc/meminfo", "r") as mf:
                        for line in mf:
                            if line.startswith(("MemTotal:", "MemAvailable:", "MemFree:", "SwapTotal:", "SwapFree:")):
                                k, v = line.split(":", 1)
                                mem_info[k.strip()] = v.strip()
                                if len(mem_info) >= 5:
                                    break
                except Exception:
                    pass
                # #endregion
                _dbg(
                    "mission_centralised.py:_wait_all",
                    f"polling drones ({context})",
                    {"elapsed_s": round(now - start, 2), "context": context, "drones": statuses, "mem": mem_info},
                    hypothesis_id="H1,H2,H3",
                )
                # #endregion
                last_log = now
            time.sleep(0.05)
        # #region agent log
        _dbg(
            "mission_centralised.py:_wait_all",
            f"TIMEOUT ({context})",
            {"timeout_s": timeout_s, "context": context},
            hypothesis_id="H1",
        )
        # #endregion
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

        def _arm_one(d: MissionDrone) -> Tuple[str, bool, bool, Dict[str, Any]]:
            arm_ok = False
            for attempt in range(3):
                try:
                    arm_ok = bool(d.arm())
                    if arm_ok:
                        break
                except Exception as exc:
                    _dbg("mission_centralised.py:arm_and_offboard",
                         f"{d.namespace} arm attempt {attempt} raised",
                         {"error": repr(exc)}, hypothesis_id="H2")
                time.sleep(0.4 * (attempt + 1))
            off_ok = False
            for attempt in range(3):
                try:
                    off_ok = bool(d.offboard())
                    if off_ok:
                        break
                except Exception as exc:
                    _dbg("mission_centralised.py:arm_and_offboard",
                         f"{d.namespace} offboard attempt {attempt} raised",
                         {"error": repr(exc)}, hypothesis_id="H2")
                time.sleep(0.4 * (attempt + 1))
            info_min: Dict[str, Any] = {}
            try:
                info = d.info
                info_min = {k: info.get(k) for k in ("connected", "armed", "offboard", "state")}
            except Exception:
                pass
            return (d.namespace, bool(arm_ok), bool(off_ok), info_min)

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            results = list(ex.map(_arm_one, self.drones))
        ok = all(a and o for _, a, o, _ in results)
        elapsed = round(time.time() - t0, 2)
        _dbg(
            "mission_centralised.py:arm_and_offboard",
            "parallel arm+offboard done",
            {"ok": ok, "elapsed_s": elapsed,
             "per_drone": [{"ns": n, "arm": a, "off": o, "info": i} for n, a, o, i in results]},
            hypothesis_id="H2",
        )
        if not ok:
            _dbg(
                "mission_centralised.py:arm_and_offboard",
                "arm/offboard FAILED detail",
                {"elapsed_s": elapsed,
                 "per_drone": [{"ns": n, "arm": a, "off": o, "info": i} for n, a, o, i in results]},
                hypothesis_id="H2",
            )
        return ok

    def takeoff(self, height: float = 1.2, speed: float = 0.8) -> bool:
        if self.dry_run:
            return True
        for drone in self.drones:
            # #region agent log
            try:
                info = drone.info
                info_min = {k: info.get(k) for k in ("connected", "armed", "offboard", "state")}
            except Exception:
                info_min = {}
            _dbg(
                "mission_centralised.py:takeoff",
                f"{drone.namespace} pre-takeoff info",
                {"ns": drone.namespace, "info": info_min, "height": height, "speed": speed},
                hypothesis_id="H2,H4",
            )
            # #endregion
            drone.takeoff_async(height=height, speed=speed)
            # #region agent log
            beh = drone.current_behavior
            try:
                st = int(beh.status) if beh is not None else None
            except Exception:
                st = "err"
            _dbg(
                "mission_centralised.py:takeoff",
                f"{drone.namespace} post-takeoff_async behavior status",
                {"ns": drone.namespace, "beh_status": st, "beh_is_none": beh is None},
                hypothesis_id="H1,H5",
            )
            # #endregion
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

        t0 = time.time()
        with cf.ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            primed = list(ex.map(_prime, zip(self.drones, spawn_xy)))
        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "PositionMotion primed (parallel)",
            {"elapsed_s": round(time.time() - t0, 2), "primed": primed,
             "spawn": [list(s) for s in spawn_xy]},
            hypothesis_id="H3",
        )
        if not all(ok for _, ok in primed):
            _dbg("mission_centralised.py:streaming_takeoff",
                 "priming failed for some drones; aborting", {"primed": primed}, hypothesis_id="H3")
            return False

        # 1b. Wait until every drone reports z>deck (or timeout). Runtime
        # evidence (term 15): drone0/drone1 logged Set control mode success
        # ~3–6 s AFTER drone3/4 — ramp started immediately and those two
        # never reached target_z in the final check ("only three took off").
        t_deck = time.time()
        deck_z = 0.22
        deck_ok = False
        while time.time() - t_deck < 12.0:
            try:
                if all(float(d.position[2]) > deck_z for d in self.drones):
                    deck_ok = True
                    break
            except Exception:
                pass
            time.sleep(0.15)
        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "post-prime deck wait done",
            {"wait_s": round(time.time() - t_deck, 2),
             "deck_ok": deck_ok,
             "zs": [round(float(d.position[2]), 3) for d in self.drones]},
            hypothesis_id="H3",
        )

        # 2. Ramp z from each drone's spawn altitude to target_z.
        start_z = max(0.0, min(s[2] for s in spawn_xy))
        distance = max(0.1, target_z - start_z)
        ramp_duration = max(2.0, distance / max(0.1, climb_rate) + 1.0)
        rate_hz = 20.0
        dt = 1.0 / rate_hz

        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "ramp phase start",
            {"start_z": round(start_z, 3), "target_z": target_z,
             "climb_rate": climb_rate, "ramp_duration_s": round(ramp_duration, 2)},
            hypothesis_id="H9",
        )

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

        # 4. Verify altitude on every drone.
        final_zs = []
        all_ok = True
        for d in self.drones:
            try:
                z_real = float(d.position[2])
            except Exception:
                z_real = float("nan")
            final_zs.append(z_real)
            if not math.isfinite(z_real) or abs(z_real - target_z) > 0.4:
                all_ok = False
        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "final altitude check",
            {"target_z": target_z, "final_zs": [round(z, 3) for z in final_zs], "all_ok": all_ok},
            hypothesis_id="H3",
        )
        return all_ok

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
        _dbg("mission_centralised.py:streaming_land",
             "descent finished",
             {"duration_s": round(duration, 2)}, hypothesis_id="H9")

        self._reset_platform_state(source="streaming_land")
        return True

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
        from concurrent.futures import ThreadPoolExecutor

        def _reset_one(d):
            out = {"ns": d.namespace, "manual": None, "disarm": None}
            for attempt in range(3):
                try:
                    if bool(d.manual()):
                        out["manual"] = True
                        break
                except Exception as exc:
                    out["manual"] = f"err:{exc!r}"
                time.sleep(0.3)
            for attempt in range(3):
                try:
                    if bool(d.disarm()):
                        out["disarm"] = True
                        break
                except Exception as exc:
                    out["disarm"] = f"err:{exc!r}"
                time.sleep(0.3)
            return out

        t_r = time.time()
        with ThreadPoolExecutor(max_workers=max(1, len(self.drones))) as ex:
            futs = [ex.submit(_reset_one, d) for d in self.drones]
            reset_results = []
            for f in futs:
                try:
                    reset_results.append(f.result(timeout=5.0))
                except Exception as exc:
                    reset_results.append({"error": repr(exc)})
        _dbg("mission_centralised.py:_reset_platform_state",
             "platform reset (manual + disarm)",
             {"source": source,
              "elapsed_s": round(time.time() - t_r, 2),
              "per_drone": reset_results},
             hypothesis_id="H9")

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

    def _estimate_collisions(self, points: Sequence[Vec3], static_obstacles: Sequence[Tuple[float, float, float]]) -> int:
        collisions = 0
        for px, py, _ in points:
            for ox, oy, radius in static_obstacles:
                if ((px - ox) ** 2 + (py - oy) ** 2) ** 0.5 <= radius:
                    collisions += 1
                    break
        return collisions

    def run_plan(
        self,
        stage_name: str,
        stage_plan: StagePlan,
        speed: float,
        transition_points: int = 4,
        static_obstacles: Optional[Sequence[Tuple[float, float, float]]] = None,
        wait_for_completion: bool = True,
        command_period_s: float = 0.25,
    ) -> StageMetrics:
        metrics = StageMetrics(stage=stage_name, planned_waypoints=len(stage_plan.waypoints))
        metrics.start_time = time.time()
        if not stage_plan.waypoints:
            metrics.end_time = time.time()
            self.metrics[stage_name] = metrics
            return metrics

        previous_formation = stage_plan.formation_names[0]
        MIN_YAW_DISP = 0.3  # m; segments shorter than this do not define heading
        last_valid_yaw: Optional[float] = None

        def _segment_yaw(a_idx: int, b_idx: int) -> Optional[float]:
            if a_idx < 0 or b_idx >= len(stage_plan.waypoints):
                return None
            pa = stage_plan.waypoints[a_idx]
            pb = stage_plan.waypoints[b_idx]
            if math.hypot(pb[0] - pa[0], pb[1] - pa[1]) < MIN_YAW_DISP:
                return None
            return heading_between(pa, pb)

        for idx, leader_target in enumerate(stage_plan.waypoints):
            # Use the heading of the *current* segment (prev -> current). If we
            # used the next segment instead, the follower formation would pre-
            # rotate toward the subsequent waypoint and, near narrow gaps (e.g.
            # after window1 in stage2) push the rear follower sideways into the
            # wall.
            yaw: Optional[float] = None
            if idx > 0:
                yaw = _segment_yaw(idx - 1, idx)
            if yaw is None and idx + 1 < len(stage_plan.waypoints):
                yaw = _segment_yaw(idx, idx + 1)
            if yaw is None:
                # Fallback: walk further back for a meaningful displacement
                # (handles near-stationary descent points where consecutive xy
                # are identical and would otherwise flip the column).
                back = idx - 2
                while yaw is None and back >= 0:
                    yaw = _segment_yaw(back, idx)
                    back -= 1
            if yaw is None:
                yaw = last_valid_yaw if last_valid_yaw is not None else 0.0
            last_valid_yaw = yaw

            formation = stage_plan.formation_names[min(idx, len(stage_plan.formation_names) - 1)]
            alpha = 1.0
            if formation != previous_formation:
                alpha = min(1.0, (idx % transition_points + 1) / float(transition_points))
            follower_targets = self.controller.compute_targets(
                leader_position=leader_target,
                leader_yaw=yaw,
                formation_name=formation,
                previous_formation_name=previous_formation,
                transition_alpha=alpha,
                static_obstacles=static_obstacles,
            )
            # #region agent log
            _dbg(
                location="mission_centralised.py:run_plan",
                message="waypoint targets",
                hypothesis_id="H5",
                run_id="post-fix",
                data={
                    "stage": stage_name,
                    "idx": idx,
                    "leader_target": list(leader_target),
                    "yaw_deg": math.degrees(yaw),
                    "formation": formation,
                    "prev_formation": previous_formation,
                    "alpha": alpha,
                    "follower_targets": [list(t) for t in follower_targets],
                },
            )
            # #endregion
            success = self._command_swarm_step(
                leader_target=leader_target,
                follower_targets=follower_targets,
                speed=speed,
                yaw_mode=YawMode.PATH_FACING,
                yaw_angle=None,
                wait_for_completion=wait_for_completion,
            )
            if not success:
                self._log(f"[{stage_name}] command timeout at waypoint {idx}")
                break

            metrics.completed_waypoints += 1
            metrics.collisions += self._estimate_collisions(follower_targets, static_obstacles or [])
            previous_formation = formation
            if not wait_for_completion and not self.dry_run:
                time.sleep(command_period_s)
        metrics.end_time = time.time()
        self.metrics[stage_name] = metrics
        return metrics

    def run_stage1(self) -> StageMetrics:
        """Stage1 entry point.

        Previously used ``run_plan`` with 24 discrete ``go_to`` waypoints, which
        produced the "step-by-step" look (each waypoint was a blocking behavior
        call that waited for the full swarm to arrive before advancing). This
        wrapper now defers to ``run_stage1_streaming`` which drives the leader
        along a continuous arc at 10 Hz using the same ``PositionMotion``
        streaming path that stage4 uses, giving the same smooth motion while
        still cycling through the formation schedule.

        If the motion-reference handler fails to load (dry-run, missing
        publisher, etc.) the streaming method itself falls back to the legacy
        ``run_stage1_legacy`` so behaviour is preserved on non-streaming setups.
        """
        return self.run_stage1_streaming()

    def run_stage1_legacy(self) -> StageMetrics:
        """Legacy discrete-waypoint version of stage1 (kept as fallback)."""
        return self.run_plan(
            "stage1",
            self.planner.plan_stage1(),
            speed=0.6,
            transition_points=3,
        )

    def run_stage1_streaming(self) -> StageMetrics:
        """Stage1: continuous 10 Hz streaming around the circular trajectory.

        Architecture (mirrors stage4's Phase-3 loop, with a new pre-rally
        phase to solve the "everyone crashes together on takeoff" problem
        that namespace-ordered slot assignment caused in the previous
        revision):

          Phase 0 -- Prime:
              Load ``PositionMotion`` on all drones. Stream a stationary
              hold set-point at each drone's CURRENT (x, y, cruise_z) so
              the first real slew target isn't a teleport.
          Phase 1 -- Rally-in-place (NEW):
              Compute the initial ``line`` formation slots AROUND the
              leader's current position (leader stays put). Use greedy
              nearest-neighbour assignment so each follower goes to the
              slot closest to its actual spawn position. This eliminates
              the crossing-paths crashes caused by namespace-ordered
              assignment (previously drone3@(0,+1) was sent through
              drone4@(0,-1) and vice versa).
          Phase 2 -- Ferry to ring entry:
              Slew the leader from its current XY to the nearest point on
              the stage1 ring, followers follow the ``line`` formation.
              ``slew_speed`` is capped at ``twist_limit_xy`` so the drones
              can physically keep up -- the previous revision asked for
              1.5 m/s while twist_limit was 0.8 m/s, so drones never
              reached the ring before cruise started and ended up
              chord-cutting the circle (user saw "half a loop then
              landed").
          Phase 3 -- Continuous circular sweep (10 Hz stream):
              The leader target sweeps around the ring at a fixed angular
              speed ``omega``. Yaw tracks the tangent direction. Every
              100 ms we compute follower targets via
              ``FollowerController`` and publish pose references directly
              to ``motion_reference/pose``.
              ``omega`` is chosen so that the ring linear speed
              (omega * radius) is WELL below ``twist_limit_xy``, and
              ``num_loops > 1`` gives the drones time to visibly close
              the circle even with 1-2 s of initial lag.
          Phase 4 -- Hover:
              Stream stationary references at the final poses for ~1 s.

        Along the way we sample per-tick telemetry (leader XY, follower
        XY, commanded follower target, active formation) and compute
        stage1-specific analytical metrics at the end:
            * ``formation_rmse_mean``  follower-target error RMS (m)
            * ``min_pair_distance_m``  worst-case pairwise min distance
            * ``completion_ratio``     actual leader angular travel / planned
            * ``leader_path_length_m`` total leader arc length (m)
            * ``formation_switches``   number of formation transitions
        These are stored on ``StageMetrics`` and emitted by
        ``export_metrics`` as a stage1-specific CSV.
        """
        stage_name = "stage1"
        stage = self.scenario.stage1
        metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
        metrics.start_time = time.time()
        if stage is None:
            metrics.end_time = time.time()
            self.metrics[stage_name] = metrics
            return metrics

        if not self.dry_run:
            if not self._ensure_motion_ref_loaded():
                self._log("[stage1] motion_ref_handler load failed; falling back to legacy go_to plan")
                return self.run_stage1_legacy()

        cx, cy = stage.stage_center
        radius = stage.diameter / 2.0
        z = self.planner.cruise_height  # 1.2 m

        formations = self.planner._stage1_formations(stage.formations)
        num_forms = max(1, len(formations))

        # ---- Loop parameters (aligned with the drone's actual twist capability)
        rate_hz = 10.0
        dt = 1.0 / rate_hz
        # Twist limits below are aligned with the nominal cruise linear
        # speed (omega * radius). Slew and rally use slightly higher xy
        # twist to approach the ring quickly; cruise uses higher xy twist
        # (0.8 m/s) so followers can catch up with the leader AND absorb
        # the transverse motion required by formation blends -- the
        # previous 0.5 m/s twist produced a 1.49 m formation RMSE because
        # followers could barely keep up with the 0.30 m/s ring speed,
        # let alone reshape mid-flight.
        twist_slew = [0.6, 0.6, 0.3]
        twist_cruise = [0.8, 0.8, 0.4]
        twist_rally = [0.5, 0.5, 0.3]
        # omega = 0.20 rad/s * radius = 0.30 m/s line speed. Followers
        # now have ~2.6x headroom (0.8 m/s twist vs 0.30 m/s ring speed)
        # to reshape during formation blends without falling behind.
        omega = 0.20  # rad/s
        # User request: change formation ONCE PER HALF LAP so each
        # formation is visibly held for a long stretch of the ring
        # (previously all 6 formations were crammed into 1.2 loops ->
        # each one lasted ~6 s and was barely recognisable).
        #   formation_period = pi / omega = 15.7 s (half a lap)
        # To ensure EVERY planned formation is shown at least once, cruise
        # for num_forms half-laps, i.e. num_loops = num_forms * 0.5, but
        # always fly at least 1.5 loops so a single-formation scenario
        # still produces a full ring video.
        formation_period = math.pi / omega
        num_loops = max(1.5, num_forms * 0.5)
        total_angle = 2.0 * math.pi * num_loops
        T_cruise = total_angle / omega
        # Gentler transitions: ~4 s of linear blending at each formation
        # change (vs 2.5 s before). With the new 15.7 s period this is
        # still only ~25% of the hold time so the target formation is
        # clearly visible for the remaining 75%.
        transition_s = min(4.0, formation_period * 0.3)

        # ---- Pick theta0 = angle of leader's current position w.r.t. ring
        try:
            if not self.dry_run:
                lp = self.drones[0].position
                lp_x, lp_y = float(lp[0]), float(lp[1])
            else:
                lp_x, lp_y = cx + radius, cy
        except Exception:
            lp_x, lp_y = cx + radius, cy
        dx0 = lp_x - cx
        dy0 = lp_y - cy
        if math.hypot(dx0, dy0) < 1e-3:
            theta0 = 0.0
        else:
            theta0 = math.atan2(dy0, dx0)

        entry_x = cx + radius * math.cos(theta0)
        entry_y = cy + radius * math.sin(theta0)
        slew_dist = math.hypot(entry_x - lp_x, entry_y - lp_y)
        # Cap slew_speed at twist_slew[0] so the drones can actually keep
        # up with the commanded target (critical fix: prev revision used
        # 1.5 m/s while twist was 0.8 m/s, so drones fell 50% behind).
        slew_speed = min(0.6, twist_slew[0])
        T_slew = max(1.0, slew_dist / slew_speed) if slew_dist > 0.05 else 0.0
        slew_yaw = math.atan2(entry_y - lp_y, entry_x - lp_x) if slew_dist > 1e-3 else (theta0 + math.pi / 2.0)

        # ---- Greedy slot-assignment for the initial ``line`` formation
        # The FollowerController returns 4 offsets in a fixed order, rotated
        # by the leader's heading. We ALSO need to decide which physical
        # drone (drone1..drone4) flies which slot. Namespace-ordered
        # assignment causes crossing paths -- see docstring. Compute
        # nearest-neighbour assignment ONCE here, then freeze it for the
        # whole stage1 run so slots don't swap mid-flight.
        default_spacing = self.controller.default_spacing
        # Line offsets in drone-local frame (x along nose, y left).
        # orbit's leader-crossing fix is now baked into
        # ``centralised/formations.py`` (diagonal slot layout), so stage1
        # no longer needs a local override -- any stage that imports
        # ``get_formation_offsets`` will get the collision-safe orbit.
        from centralised.formations import get_formation_offsets as _get_offsets
        from centralised.utils import rotate_xy as _rotate_xy
        line_offsets_local = _get_offsets("line", spacing=default_spacing, z=0.0)
        line_slots_global: List[Tuple[float, float]] = []
        for off in line_offsets_local:
            dx, dy = _rotate_xy(off[0], off[1], slew_yaw)
            line_slots_global.append((lp_x + dx, lp_y + dy))
        follower_positions_now: List[Tuple[float, float]] = []
        if not self.dry_run:
            for d in self.drones[1:]:
                try:
                    p = d.position
                    follower_positions_now.append((float(p[0]), float(p[1])))
                except Exception:
                    follower_positions_now.append((lp_x, lp_y))
        else:
            follower_positions_now = [(lp_x + i * 0.3, lp_y) for i in range(4)]
        slot_to_follower = _greedy_slot_assignment(follower_positions_now, line_slots_global)
        # follower_index (0..3) -> self.drones[1 + follower_index]
        # slot_to_drone_idx[slot] = follower_idx (0..3).
        # Invert to drone_to_slot[follower_idx] = slot.
        drone_to_slot: List[int] = [0] * 4
        for slot_i, drone_idx in enumerate(slot_to_follower):
            if 0 <= drone_idx < 4:
                drone_to_slot[drone_idx] = slot_i

        _dbg(
            "mission_centralised.py:run_stage1_streaming",
            "plan",
            {
                "ring_center": [cx, cy],
                "radius": radius,
                "z": z,
                "formations": formations,
                "num_forms": num_forms,
                "omega_rad_s": omega,
                "line_speed_m_s": round(omega * radius, 3),
                "T_cruise_s": round(T_cruise, 2),
                "formation_period_s": round(formation_period, 2),
                "transition_s": round(transition_s, 2),
                "theta0_deg": round(math.degrees(theta0), 1),
                "entry_xy": [round(entry_x, 3), round(entry_y, 3)],
                "slew_dist_m": round(slew_dist, 3),
                "T_slew_s": round(T_slew, 2),
                "slew_speed_m_s": slew_speed,
                "twist_slew": twist_slew,
                "twist_cruise": twist_cruise,
                "initial_line_slots_global": [[round(p[0], 3), round(p[1], 3)] for p in line_slots_global],
                "follower_positions_now": [[round(p[0], 3), round(p[1], 3)] for p in follower_positions_now],
                "slot_to_follower_idx": slot_to_follower,
                "drone_to_slot": drone_to_slot,
            },
            hypothesis_id="H10",
        )

        tick_count = 0
        final_leader: Vec3 = (entry_x, entry_y, z)
        final_followers_indexed: List[Vec3] = [(lp_x, lp_y, z) for _ in range(4)]

        # ---- Per-tick telemetry buffers for post-run metrics.
        position_err_sqsum = 0.0       # sum of squared (actual - target) xy distances
        position_err_samples = 0
        min_pair_dist_seen = float("inf")
        # Official collision-avoidance metric: number of ticks during which
        # ANY pair of drones was closer than PAIR_SAFETY_M. 0 => no close
        # calls at any point during the cruise.
        PAIR_SAFETY_M = 0.30
        PAIR_PHYSICAL_M = 0.15  # physical body + propeller radius sum
        pair_collision_ticks = 0
        pair_ticks_physical = 0
        leader_path_len = 0.0
        last_leader_xy: Optional[Tuple[float, float]] = None
        cruise_theta_start: Optional[float] = None
        cruise_theta_last: Optional[float] = None
        cruise_theta_travel = 0.0
        formation_switch_count = 0
        # Official reconfigurability metric: set of formation names the
        # cruise loop actually commanded (and therefore visibly displayed).
        formations_visited_set: set = set()

        def _publish_swarm(leader_tgt: Vec3, slots_in_slot_order: List[Vec3],
                           yaw_cmd: float, twist: List[float]) -> None:
            """Send leader + each follower's assigned slot target.

            ``slots_in_slot_order[slot_i]`` is the WORLD target of slot i.
            ``self.drones[1 + follower_idx]`` gets the slot that follower
            is assigned to via ``drone_to_slot``.
            """
            if self.dry_run:
                return
            self._stream_position(self.drones[0], leader_tgt, yaw_cmd, twist)
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if 0 <= slot_i < len(slots_in_slot_order):
                    ft = slots_in_slot_order[slot_i]
                    self._stream_position(self.drones[1 + follower_idx], ft, yaw_cmd, twist)

        def _sample_telemetry(commanded_slots: List[Vec3]) -> None:
            nonlocal position_err_sqsum, position_err_samples
            nonlocal min_pair_dist_seen, pair_collision_ticks, pair_ticks_physical
            if self.dry_run:
                return
            # Formation RMSE: per-follower |actual - commanded slot|
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if not (0 <= slot_i < len(commanded_slots)):
                    continue
                tgt = commanded_slots[slot_i]
                try:
                    p = self.drones[1 + follower_idx].position
                    err2 = (float(p[0]) - tgt[0]) ** 2 + (float(p[1]) - tgt[1]) ** 2
                    position_err_sqsum += err2
                    position_err_samples += 1
                except Exception:
                    pass
            # Minimum pairwise XY distance across the whole swarm (this tick).
            tick_min_pair = float("inf")
            try:
                swarm_xy: List[Tuple[float, float]] = []
                for d in self.drones:
                    p = d.position
                    swarm_xy.append((float(p[0]), float(p[1])))
                for i in range(len(swarm_xy)):
                    for j in range(i + 1, len(swarm_xy)):
                        d2 = math.hypot(swarm_xy[i][0] - swarm_xy[j][0], swarm_xy[i][1] - swarm_xy[j][1])
                        if d2 < tick_min_pair:
                            tick_min_pair = d2
            except Exception:
                pass
            if tick_min_pair < min_pair_dist_seen:
                min_pair_dist_seen = tick_min_pair
            if tick_min_pair < PAIR_SAFETY_M:
                pair_collision_ticks += 1
            if tick_min_pair < PAIR_PHYSICAL_M:
                pair_ticks_physical += 1

        # ---- Phase 0: prime pipeline at current drone XY, cruise z.
        if not self.dry_run:
            for drone in self.drones:
                try:
                    cur = drone.position
                    px = float(cur[0]); py = float(cur[1])
                except Exception:
                    px, py = lp_x, lp_y
                self._stream_position(drone, (px, py, z), slew_yaw, twist_slew)
            time.sleep(0.5)

        # ---- Phase 1: Rally in place (leader stationary, followers go to line slots)
        T_rally = 3.0
        if not self.dry_run and T_rally > 0.0:
            rally_slots_xyz: List[Vec3] = [(p[0], p[1], z) for p in line_slots_global]
            t0 = time.time()
            while True:
                e = time.time() - t0
                if e >= T_rally:
                    break
                _publish_swarm((lp_x, lp_y, z), rally_slots_xyz, slew_yaw, twist_rally)
                tick_count += 1
                final_leader = (lp_x, lp_y, z)
                final_followers_indexed = rally_slots_xyz
                time.sleep(dt)
            self._log("[stage1] Rally complete, leader hold at spawn, followers in LINE")

        # ---- Phase 2: ferry to ring entry
        if T_slew > 0.0:
            t0 = time.time()
            while True:
                e = time.time() - t0
                if e >= T_slew:
                    break
                frac = e / T_slew
                lx = lp_x + (entry_x - lp_x) * frac
                ly = lp_y + (entry_y - lp_y) * frac
                leader_target: Vec3 = (lx, ly, z)
                slot_targets: List[Vec3] = []
                for off in line_offsets_local:
                    dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
                    slot_targets.append((lx + dxg, ly + dyg, z + off[2]))
                _publish_swarm(leader_target, slot_targets, slew_yaw, twist_slew)
                tick_count += 1
                final_leader = leader_target
                final_followers_indexed = slot_targets
                time.sleep(dt)
            self._log(f"[stage1] Ferry complete, leader at ring entry ({entry_x:.2f}, {entry_y:.2f})")

        # ---- Phase 3: cruise around ring with scheduled formations
        previous_formation = formations[0]
        last_log = 0.0
        t_start = time.time()
        while True:
            t_rel = time.time() - t_start
            if t_rel >= T_cruise:
                break

            theta = theta0 + omega * t_rel
            lx = cx + radius * math.cos(theta)
            ly = cy + radius * math.sin(theta)
            yaw = theta + math.pi / 2.0
            leader_target = (lx, ly, z)

            # Scheduled formation + linear blend at segment boundary.
            seg_float = t_rel / formation_period
            seg_idx = int(seg_float) % num_forms
            seg_t = t_rel - seg_idx * formation_period
            formation = formations[seg_idx]
            prev_name = formations[(seg_idx - 1) % num_forms] if seg_idx > 0 else formations[0]
            if formation != prev_name and seg_t < transition_s:
                alpha = max(0.0, min(1.0, seg_t / transition_s))
            else:
                alpha = 1.0

            # Compute slot targets directly (so we publish per drone_to_slot).
            cur_offs_local = _get_offsets(formation, spacing=default_spacing, z=0.0)
            if formation != prev_name and alpha < 1.0:
                prev_offs_local = _get_offsets(prev_name, spacing=default_spacing, z=0.0)
                blended_local: List[Tuple[float, float, float]] = []
                for po, co in zip(prev_offs_local, cur_offs_local):
                    blended_local.append(
                        (po[0] + (co[0] - po[0]) * alpha,
                         po[1] + (co[1] - po[1]) * alpha,
                         po[2] + (co[2] - po[2]) * alpha)
                    )
                offs_local = blended_local
            else:
                offs_local = list(cur_offs_local)
            slot_targets = []
            for off in offs_local:
                dxg, dyg = _rotate_xy(off[0], off[1], yaw)
                slot_targets.append((lx + dxg, ly + dyg, z + off[2]))

            _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
            _sample_telemetry(slot_targets)

            # Track leader actual path length and angular travel.
            if not self.dry_run:
                try:
                    p = self.drones[0].position
                    axy = (float(p[0]), float(p[1]))
                    if last_leader_xy is not None:
                        leader_path_len += math.hypot(axy[0] - last_leader_xy[0], axy[1] - last_leader_xy[1])
                    last_leader_xy = axy
                    cur_theta = math.atan2(axy[1] - cy, axy[0] - cx)
                    if cruise_theta_start is None:
                        cruise_theta_start = cur_theta
                        cruise_theta_last = cur_theta
                    else:
                        d_theta = cur_theta - (cruise_theta_last or cur_theta)
                        if d_theta > math.pi:
                            d_theta -= 2 * math.pi
                        elif d_theta < -math.pi:
                            d_theta += 2 * math.pi
                        cruise_theta_travel += d_theta
                        cruise_theta_last = cur_theta
                except Exception:
                    pass

            tick_count += 1
            metrics.completed_waypoints = tick_count
            final_leader = leader_target
            final_followers_indexed = slot_targets

            if formation != previous_formation:
                self._log(f"[stage1] FORMATION -> {formation.upper()}")
                formation_switch_count += 1
            previous_formation = formation
            formations_visited_set.add(formation)

            if time.time() - last_log >= 1.0:
                last_log = time.time()
                _dbg(
                    "mission_centralised.py:run_stage1_streaming",
                    "cruise tick",
                    {
                        "t_s": round(t_rel, 2),
                        "theta_deg": round(math.degrees(theta), 1),
                        "leader_target": [round(lx, 3), round(ly, 3), round(z, 3)],
                        "leader_actual": [round(float(self.drones[0].position[0]), 3), round(float(self.drones[0].position[1]), 3), round(float(self.drones[0].position[2]), 3)] if not self.dry_run else None,
                        "formation": formation,
                        "alpha": round(alpha, 2),
                    },
                    hypothesis_id="H10",
                )

            time.sleep(dt)

        # ---- Phase 4: hover at final pose
        if not self.dry_run:
            hover_yaw = 0.0
            hover_twist = [0.2, 0.2, 0.2]
            t0 = time.time()
            while time.time() - t0 < 1.0:
                _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
                time.sleep(dt)

        # ---- Finalise metrics
        metrics.planned_waypoints = max(tick_count, metrics.planned_waypoints)
        metrics.end_time = time.time()

        # Centralised-specific analytics (detail, not pass/fail).
        if position_err_samples > 0:
            metrics.formation_rmse_mean = math.sqrt(position_err_sqsum / position_err_samples)
        if min_pair_dist_seen < float("inf"):
            metrics.min_pair_distance_m = min_pair_dist_seen
        planned_angular = abs(omega * T_cruise)
        if planned_angular > 1e-6:
            metrics.completion_ratio = max(0.0, min(1.0, abs(cruise_theta_travel) / planned_angular))
        metrics.formation_switches = formation_switch_count

        # === OFFICIAL metric axis 2: Reconfigurability =========================
        metrics.formations_planned = num_forms
        metrics.formations_visited = len(formations_visited_set)

        # === OFFICIAL metric axis 4: Collision avoidance =======================
        metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m threshold
        metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m threshold
        # Stage1 has no static/dynamic obstacles -> leave obstacle fields as None.

        # === OFFICIAL metric axis 5: Efficiency in movement ====================
        metrics.leader_path_length_m = leader_path_len
        # Ideal arc length along the commanded ring over the planned cruise.
        metrics.ideal_path_length_m = 2.0 * math.pi * radius * num_loops

        # === Time-taken breakdown ==============================================
        # 1 lap = 2pi / omega seconds (e.g. omega=0.20 -> 31.42 s).
        metrics.time_per_lap_s = (2.0 * math.pi / omega) if omega > 1e-6 else None
        metrics.cruise_time_s = T_cruise
        metrics.laps_completed = (
            abs(cruise_theta_travel) / (2.0 * math.pi)
            if cruise_theta_travel is not None
            else None
        )
        # Per-formation display duration: in stage1 every formation holds for
        # exactly ``formation_period`` seconds, repeated (num_loops * 2 / num_forms)
        # times over the cruise. Compute shown duration per unique formation name.
        num_half_laps = num_loops * 2.0
        count_per_formation: Dict[str, int] = {}
        for i in range(int(math.floor(num_half_laps))):
            name = formations[i % num_forms]
            count_per_formation[name] = count_per_formation.get(name, 0) + 1
        # Handle fractional tail (e.g. 0.5 extra half-lap).
        frac = num_half_laps - math.floor(num_half_laps)
        if frac > 1e-6:
            name = formations[int(math.floor(num_half_laps)) % num_forms]
            metrics.time_per_formation = {
                n: count_per_formation.get(n, 0) * formation_period
                for n in formations
            }
            metrics.time_per_formation[name] = (
                metrics.time_per_formation.get(name, 0.0) + frac * formation_period
            )
        else:
            metrics.time_per_formation = {
                n: count_per_formation.get(n, 0) * formation_period
                for n in formations
            }

        # === OFFICIAL metric axis 1: Success rate (dual threshold) =============
        # Stage 1 success criteria (shared by strict & physical variants):
        #   (a) completion_ratio >= 0.95  (leader actually circled the ring)
        #   (b) reconfigurability == 1.0  (every planned formation shown)
        #   (c) pair-collision tick count == 0
        # Strict:   (c) uses 0.30 m near-miss threshold  (conservative)
        # Physical: (c) uses 0.15 m body-contact threshold (lenient, matches
        #           physical crazyflie radius)
        cr = metrics.completion_ratio if metrics.completion_ratio is not None else 0.0
        recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
        metrics.success_strict = bool(
            cr >= 0.95 and recfg >= 0.999 and pair_collision_ticks == 0
        )
        metrics.success_physical = bool(
            cr >= 0.95 and recfg >= 0.999 and pair_ticks_physical == 0
        )
        # Back-compat: ``success`` mirrors the strict variant.
        metrics.success = metrics.success_strict
        metrics.success_criterion = (
            "completion_ratio>=0.95 AND reconfigurability==1.0 AND "
            "pair_collision_ticks==0 (@0.30m strict / @0.15m physical)"
        )

        self.metrics[stage_name] = metrics
        _dbg(
            "mission_centralised.py:run_stage1_streaming",
            "stage1 streaming done",
            {
                "ticks": tick_count,
                "elapsed_s": round(time.time() - metrics.start_time, 2),
                # Official metrics (what the brief asks for).
                "success": metrics.success,
                "success_criterion": metrics.success_criterion,
                "duration_sec": round(metrics.duration_sec, 2),
                "formations_planned": metrics.formations_planned,
                "formations_visited": metrics.formations_visited,
                "reconfigurability": metrics.reconfigurability,
                "num_pair_collision_ticks": metrics.num_pair_collision_ticks,
                "min_pair_distance_m": metrics.min_pair_distance_m,
                "leader_path_length_m": round(metrics.leader_path_length_m or 0.0, 3),
                "ideal_path_length_m": round(metrics.ideal_path_length_m or 0.0, 3),
                "path_efficiency": metrics.path_efficiency,
                # Centralised-specific details.
                "formation_rmse_mean": metrics.formation_rmse_mean,
                "completion_ratio": metrics.completion_ratio,
                "formation_switches": metrics.formation_switches,
                "cruise_theta_travel_rad": round(cruise_theta_travel, 3),
                "planned_angular_rad": round(planned_angular, 3),
            },
            hypothesis_id="H10",
        )
        return metrics

    def run_stage2(self) -> StageMetrics:
        """Stage2 entry point. Defer to the streaming variant (same smooth
        motion path that stage1 and stage4 use). If PositionMotion cannot be
        loaded (dry-run, missing publisher, ...), ``run_stage2_streaming``
        itself falls back to ``run_stage2_legacy``.
        """
        return self.run_stage2_streaming()

    def run_stage2_legacy(self) -> StageMetrics:
        """Legacy discrete-waypoint version of stage2 (kept as fallback)."""
        return self.run_plan("stage2", self.planner.plan_stage2(), speed=0.30, transition_points=3)

    def run_stage2_streaming(self) -> StageMetrics:
        """Stage2: continuous 10 Hz streaming through the wall windows.

        Architecture mirrors ``run_stage1_streaming`` + ``run_stage4``:

          Phase 0 -- Prime PositionMotion with each drone's current XY at
                     cruise_height so the first slew target isn't a teleport.
          Phase 1 -- Rally-in-place in the plan's initial formation (columnn)
                     with greedy nearest-neighbour slot assignment, to
                     prevent the takeoff-cluster crossing-paths problem.
          Phase 2 -- Ferry slew from current leader XY to the plan's entry
                     waypoint (still in columnn).
          Phase 3 -- Streaming cruise along the polyline returned by
                     ``plan_stage2()`` at a fixed linear speed. Formation
                     follows the per-segment formation name, blended over
                     ``transition_s`` seconds at each change.
          Phase 4 -- Hover at the final pose for 1 s.

        Along the way we sample per-tick telemetry identical in spirit to
        stage1 (formation RMSE, min pair distance, pair-collision ticks)
        PLUS a stage2-specific obstacle-collision detector that counts
        ticks where any drone crosses a wall outside of its gap.
        """
        stage_name = "stage2"
        stage = self.scenario.stage2
        metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
        metrics.start_time = time.time()
        if stage is None:
            metrics.end_time = time.time()
            self.metrics[stage_name] = metrics
            return metrics

        if not self.dry_run:
            if not self._ensure_motion_ref_loaded():
                self._log("[stage2] motion_ref handler load failed; falling back to legacy go_to plan")
                return self.run_stage2_legacy()

        plan = self.planner.plan_stage2()
        if not plan.waypoints:
            self._log("[stage2] empty plan; falling back to legacy")
            return self.run_stage2_legacy()

        cruise_z = self.planner.cruise_height
        default_spacing = self.controller.default_spacing

        rate_hz = 10.0
        dt = 1.0 / rate_hz
        target_speed = 0.30   # leader linear speed along polyline (m/s)
        slew_speed = 0.40     # m/s during ferry
        rally_duration = 2.5
        hover_duration = 1.0
        transition_s = 1.5    # formation blend duration between segments
        twist_slew = [0.6, 0.6, 0.5]
        twist_cruise = [0.6, 0.6, 0.5]
        twist_rally = [0.4, 0.4, 0.3]
        PAIR_SAFETY_M = 0.30
        PAIR_PHYSICAL_M = 0.15
        DRONE_RADIUS = 0.12

        from centralised.formations import get_formation_offsets as _get_offsets
        from centralised.utils import rotate_xy as _rotate_xy

        # ---- Leader current position (used for rally / ferry start)
        try:
            if not self.dry_run:
                lp = self.drones[0].position
                lp_x, lp_y = float(lp[0]), float(lp[1])
            else:
                lp_x, lp_y = 0.0, 0.0
        except Exception:
            lp_x, lp_y = 0.0, 0.0

        entry = plan.waypoints[0]
        entry_x, entry_y, entry_z = entry
        if math.hypot(entry_x - lp_x, entry_y - lp_y) > 0.05:
            slew_yaw = math.atan2(entry_y - lp_y, entry_x - lp_x)
        else:
            slew_yaw = 0.0

        # ---- Greedy slot assignment for the initial columnn rally
        initial_form = plan.formation_names[0].lower() if plan.formation_names else "columnn"
        rally_offsets_local = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
        rally_slots_global_xy: List[Tuple[float, float]] = []
        for off in rally_offsets_local:
            dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
            rally_slots_global_xy.append((lp_x + dxg, lp_y + dyg))

        follower_positions_now: List[Tuple[float, float]] = []
        if not self.dry_run:
            for d in self.drones[1:]:
                try:
                    p = d.position
                    follower_positions_now.append((float(p[0]), float(p[1])))
                except Exception:
                    follower_positions_now.append((lp_x, lp_y))
        else:
            follower_positions_now = [(lp_x + i * 0.3, lp_y) for i in range(4)]

        slot_to_follower = _greedy_slot_assignment(follower_positions_now, rally_slots_global_xy)
        drone_to_slot: List[int] = [0] * 4
        for slot_i, drone_idx in enumerate(slot_to_follower):
            if 0 <= drone_idx < 4:
                drone_to_slot[drone_idx] = slot_i

        # ---- Build polyline segments from plan.waypoints[0..N-1]
        segs: List[Dict[str, object]] = []
        total_len = 0.0
        for i in range(1, len(plan.waypoints)):
            a = plan.waypoints[i - 1]
            b = plan.waypoints[i]
            seg_len = math.hypot(b[0] - a[0], b[1] - a[1]) + 0.5 * abs(b[2] - a[2])
            fname = (
                plan.formation_names[i].lower()
                if i < len(plan.formation_names)
                else plan.formation_names[-1].lower()
            )
            segs.append({
                "start_xyz": a,
                "end_xyz": b,
                "length": seg_len,
                "start_s": total_len,
                "formation": fname,
            })
            total_len += seg_len
        if total_len < 1e-6 or not segs:
            self._log("[stage2] zero-length polyline; falling back to legacy")
            return self.run_stage2_legacy()

        formations_planned_set = {n.lower() for n in plan.formation_names}
        formations_visited_set: set = set()

        # ---- Wall obstacle model for stage2 collision detection
        walls: List[Dict[str, float]] = []
        for w in stage.windows:
            walls.append({
                "y": w.center_global_xy[1],
                "gap_x_center": w.center_global_xy[0],
                "gap_half": w.gap_width / 2.0,
                "thickness": w.thickness,
                "z_min": w.distance_floor,
                "z_max": w.distance_floor + w.height,
            })

        def _wall_hit(px: float, py: float, pz: float) -> Tuple[bool, float]:
            """Return (any_wall_collision, min_clearance_to_any_wall_plane_m)."""
            min_clearance = float("inf")
            collided = False
            for wall in walls:
                dy = abs(py - wall["y"])
                if dy > (wall["thickness"] / 2.0 + 1.5):
                    continue
                if dy < (wall["thickness"] / 2.0 + DRONE_RADIUS):
                    in_gap_x = abs(px - wall["gap_x_center"]) < max(
                        0.0, wall["gap_half"] - DRONE_RADIUS
                    )
                    in_gap_z = (
                        wall["z_min"] + DRONE_RADIUS
                        < pz
                        < wall["z_max"] - DRONE_RADIUS
                    )
                    if not (in_gap_x and in_gap_z):
                        collided = True
                clearance = dy - wall["thickness"] / 2.0
                if clearance < min_clearance:
                    min_clearance = clearance
            return collided, (min_clearance if min_clearance < float("inf") else -1.0)

        # ---- Telemetry buffers
        position_err_sqsum = 0.0
        position_err_samples = 0
        min_pair_dist_seen = float("inf")
        pair_collision_ticks = 0
        pair_ticks_physical = 0
        obstacle_collision_ticks = 0
        leader_path_len = 0.0
        last_leader_xy: Optional[Tuple[float, float]] = None
        formation_switch_count = 0
        tick_count = 0

        final_leader: Vec3 = entry
        final_followers_indexed: List[Vec3] = [(lp_x, lp_y, cruise_z) for _ in range(4)]

        def _interp_polyline(s_arc: float) -> Tuple[Vec3, str, int]:
            """Map arclength ``s_arc`` (m) to (leader_xyz, formation, seg_idx)."""
            if s_arc <= 0.0:
                sg0 = segs[0]
                return (tuple(sg0["start_xyz"]), str(sg0["formation"]), 0)  # type: ignore[return-value]
            for i, sg in enumerate(segs):
                start_s = float(sg["start_s"])  # type: ignore[arg-type]
                length = float(sg["length"])    # type: ignore[arg-type]
                if s_arc < start_s + length:
                    seg_t = (s_arc - start_s) / max(1e-6, length)
                    a = sg["start_xyz"]
                    b = sg["end_xyz"]
                    x = a[0] + (b[0] - a[0]) * seg_t  # type: ignore[index]
                    y = a[1] + (b[1] - a[1]) * seg_t  # type: ignore[index]
                    z = a[2] + (b[2] - a[2]) * seg_t  # type: ignore[index]
                    return ((x, y, z), str(sg["formation"]), i)
            last = segs[-1]
            return (tuple(last["end_xyz"]), str(last["formation"]), len(segs) - 1)  # type: ignore[return-value]

        def _segment_yaw(seg_idx: int) -> float:
            """Yaw along segment seg_idx. Fall back to nearest valid segment."""
            for j in range(seg_idx, -1, -1):
                sg = segs[j]
                a = sg["start_xyz"]; b = sg["end_xyz"]  # type: ignore[assignment]
                dx = b[0] - a[0]; dy = b[1] - a[1]      # type: ignore[index]
                if math.hypot(dx, dy) >= 0.2:
                    return math.atan2(dy, dx)
            return 0.0

        def _publish_swarm(leader_tgt: Vec3, slots_in_slot_order: List[Vec3],
                           yaw_cmd: float, twist: List[float]) -> None:
            if self.dry_run:
                return
            self._stream_position(self.drones[0], leader_tgt, yaw_cmd, twist)
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if 0 <= slot_i < len(slots_in_slot_order):
                    ft = slots_in_slot_order[slot_i]
                    self._stream_position(self.drones[1 + follower_idx], ft, yaw_cmd, twist)

        def _sample_telemetry(commanded_slots: List[Vec3]) -> None:
            nonlocal position_err_sqsum, position_err_samples
            nonlocal min_pair_dist_seen, pair_collision_ticks, pair_ticks_physical
            nonlocal obstacle_collision_ticks
            if self.dry_run:
                return
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if not (0 <= slot_i < len(commanded_slots)):
                    continue
                tgt = commanded_slots[slot_i]
                try:
                    p = self.drones[1 + follower_idx].position
                    err2 = (float(p[0]) - tgt[0]) ** 2 + (float(p[1]) - tgt[1]) ** 2
                    position_err_sqsum += err2
                    position_err_samples += 1
                except Exception:
                    pass
            tick_min_pair = float("inf")
            any_wall_hit = False
            try:
                swarm_xyz: List[Tuple[float, float, float]] = []
                for d in self.drones:
                    p = d.position
                    swarm_xyz.append((float(p[0]), float(p[1]), float(p[2])))
                for i in range(len(swarm_xyz)):
                    for j in range(i + 1, len(swarm_xyz)):
                        d_pair = math.hypot(
                            swarm_xyz[i][0] - swarm_xyz[j][0],
                            swarm_xyz[i][1] - swarm_xyz[j][1],
                        )
                        if d_pair < tick_min_pair:
                            tick_min_pair = d_pair
                    hit, _ = _wall_hit(*swarm_xyz[i])
                    if hit:
                        any_wall_hit = True
            except Exception:
                pass
            if tick_min_pair < min_pair_dist_seen:
                min_pair_dist_seen = tick_min_pair
            if tick_min_pair < PAIR_SAFETY_M:
                pair_collision_ticks += 1
            if tick_min_pair < PAIR_PHYSICAL_M:
                pair_ticks_physical += 1
            if any_wall_hit:
                obstacle_collision_ticks += 1

        _dbg(
            "mission_centralised.py:run_stage2_streaming",
            "plan",
            {
                "num_waypoints": len(plan.waypoints),
                "num_segments": len(segs),
                "total_len_m": round(total_len, 2),
                "target_speed_m_s": target_speed,
                "T_est_s": round(total_len / target_speed, 1),
                "formations_planned": sorted(formations_planned_set),
                "walls": len(walls),
                "entry": [round(entry_x, 3), round(entry_y, 3), round(entry_z, 3)],
                "slot_to_follower_idx": slot_to_follower,
                "drone_to_slot": drone_to_slot,
            },
            hypothesis_id="H12",
        )

        # ---- Phase 0: prime pipeline at current drone XY, cruise z.
        if not self.dry_run:
            for drone in self.drones:
                try:
                    cur = drone.position
                    px = float(cur[0]); py = float(cur[1])
                except Exception:
                    px, py = lp_x, lp_y
                self._stream_position(drone, (px, py, cruise_z), slew_yaw, twist_slew)
            time.sleep(0.5)

        # ---- Phase 1: Rally in place (leader holds, followers -> columnn).
        if not self.dry_run and rally_duration > 0.0:
            rally_slots_xyz: List[Vec3] = [(p[0], p[1], cruise_z) for p in rally_slots_global_xy]
            t0 = time.time()
            while time.time() - t0 < rally_duration:
                _publish_swarm((lp_x, lp_y, cruise_z), rally_slots_xyz, slew_yaw, twist_rally)
                tick_count += 1
                final_leader = (lp_x, lp_y, cruise_z)
                final_followers_indexed = rally_slots_xyz
                time.sleep(dt)
            self._log(f"[stage2] Rally complete, formed {initial_form.upper()} at leader position")

        # ---- Phase 2: Ferry from leader's current XY to entry waypoint.
        slew_dist = math.hypot(entry_x - lp_x, entry_y - lp_y) + abs(entry_z - cruise_z)
        if slew_dist > 0.05:
            T_slew = max(1.5, slew_dist / slew_speed)
            t0 = time.time()
            while True:
                e = time.time() - t0
                if e >= T_slew:
                    break
                frac = e / T_slew
                lx = lp_x + (entry_x - lp_x) * frac
                ly = lp_y + (entry_y - lp_y) * frac
                lz = cruise_z + (entry_z - cruise_z) * frac
                leader_target: Vec3 = (lx, ly, lz)
                offs = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
                slot_targets: List[Vec3] = []
                for off in offs:
                    dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
                    slot_targets.append((lx + dxg, ly + dyg, lz + off[2]))
                _publish_swarm(leader_target, slot_targets, slew_yaw, twist_slew)
                tick_count += 1
                final_leader = leader_target
                final_followers_indexed = slot_targets
                time.sleep(dt)
            self._log(f"[stage2] Ferry complete, leader at entry ({entry_x:.2f}, {entry_y:.2f}, {entry_z:.2f})")

        # ---- Phase 3: Streaming cruise along the planned polyline.
        previous_formation = initial_form
        formations_visited_set.add(initial_form)
        s_arc = 0.0
        last_log = 0.0
        t_start = time.time()
        T_max = (total_len / target_speed) * 2.0 + 20.0
        while True:
            t_rel = time.time() - t_start
            if t_rel > T_max:
                self._log(f"[stage2] cruise timed out at t={t_rel:.1f}s, s={s_arc:.2f}/{total_len:.2f}")
                break
            s_arc += target_speed * dt
            leader_target, formation, seg_idx = _interp_polyline(s_arc)
            lx, ly, lz = leader_target
            yaw = _segment_yaw(seg_idx)

            sg = segs[seg_idx]
            prev_formation_name = (
                str(segs[seg_idx - 1]["formation"]) if seg_idx - 1 >= 0 else initial_form
            )
            seg_t_arc = s_arc - float(sg["start_s"])  # type: ignore[arg-type]
            seg_time_equiv = seg_t_arc / max(1e-6, target_speed)
            if formation != prev_formation_name and seg_time_equiv < transition_s:
                alpha = max(0.0, min(1.0, seg_time_equiv / transition_s))
            else:
                alpha = 1.0

            cur_offs = _get_offsets(formation, spacing=default_spacing, z=0.0)
            if formation != prev_formation_name and alpha < 1.0:
                prev_offs = _get_offsets(prev_formation_name, spacing=default_spacing, z=0.0)
                offs_local = [
                    (
                        po[0] + (co[0] - po[0]) * alpha,
                        po[1] + (co[1] - po[1]) * alpha,
                        po[2] + (co[2] - po[2]) * alpha,
                    )
                    for po, co in zip(prev_offs, cur_offs)
                ]
            else:
                offs_local = list(cur_offs)

            slot_targets = []
            for off in offs_local:
                dxg, dyg = _rotate_xy(off[0], off[1], yaw)
                slot_targets.append((lx + dxg, ly + dyg, lz + off[2]))

            _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
            _sample_telemetry(slot_targets)

            if not self.dry_run:
                try:
                    p = self.drones[0].position
                    axy = (float(p[0]), float(p[1]))
                    if last_leader_xy is not None:
                        leader_path_len += math.hypot(axy[0] - last_leader_xy[0], axy[1] - last_leader_xy[1])
                    last_leader_xy = axy
                except Exception:
                    pass

            tick_count += 1
            metrics.completed_waypoints = tick_count
            final_leader = leader_target
            final_followers_indexed = slot_targets

            if formation != previous_formation:
                self._log(f"[stage2] FORMATION -> {formation.upper()}")
                formation_switch_count += 1
            previous_formation = formation
            formations_visited_set.add(formation)

            if time.time() - last_log >= 1.0:
                last_log = time.time()
                _dbg(
                    "mission_centralised.py:run_stage2_streaming",
                    "cruise tick",
                    {
                        "t_s": round(t_rel, 2),
                        "s_m": round(s_arc, 2),
                        "total_s_m": round(total_len, 2),
                        "seg_idx": seg_idx,
                        "leader_target": [round(lx, 3), round(ly, 3), round(lz, 3)],
                        "formation": formation,
                        "alpha": round(alpha, 2),
                    },
                    hypothesis_id="H12",
                )

            if s_arc >= total_len:
                break
            time.sleep(dt)

        # ---- Phase 4: hover at final pose.
        if not self.dry_run:
            hover_yaw = 0.0
            hover_twist = [0.2, 0.2, 0.2]
            t0 = time.time()
            while time.time() - t0 < hover_duration:
                _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
                time.sleep(dt)

        # ---- Finalise metrics
        metrics.planned_waypoints = len(plan.waypoints)
        metrics.end_time = time.time()

        if position_err_samples > 0:
            metrics.formation_rmse_mean = math.sqrt(position_err_sqsum / position_err_samples)
        if min_pair_dist_seen < float("inf"):
            metrics.min_pair_distance_m = min_pair_dist_seen
        metrics.completion_ratio = max(0.0, min(1.0, s_arc / total_len))
        metrics.formation_switches = formation_switch_count

        # Axis 2: Reconfigurability
        metrics.formations_planned = len(formations_planned_set)
        metrics.formations_visited = len(formations_visited_set & formations_planned_set)

        # Axis 4: Collision avoidance
        metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m strict
        metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m physical
        metrics.num_obstacle_collision_ticks = obstacle_collision_ticks

        # Axis 5: Efficiency
        metrics.leader_path_length_m = leader_path_len
        metrics.ideal_path_length_m = total_len

        # Time-taken breakdown: allocate per-segment duration (seg_len / target_speed)
        # to each formation name along the planned polyline.
        metrics.cruise_time_s = total_len / target_speed if target_speed > 1e-6 else None
        tpf: Dict[str, float] = {}
        for sg in segs:
            name = str(sg["formation"])
            seg_dur = float(sg["length"]) / target_speed if target_speed > 1e-6 else 0.0  # type: ignore[arg-type]
            tpf[name] = tpf.get(name, 0.0) + seg_dur
        metrics.time_per_formation = tpf

        # Stage2-specific fairness metric: how many planned windows did the
        # leader actually pass through? A "passed" window is one whose
        # ``seg_idx`` segment was fully traversed, i.e. the leader advanced
        # s_arc past the end of that window-crossing segment.
        if self.scenario.stage2 is not None:
            metrics.windows_planned = len(self.scenario.stage2.windows)
            # Geometric counting: for each planned window, find the segment
            # whose end waypoint sits on that window's y-centre (the ``inside``
            # waypoint produced by the planner), and count the window as
            # passed iff the leader's arclength ``s_arc`` reached the end of
            # that segment. This gives one count per unique window rather
            # than one per ``columnn`` sub-segment (pre/inside/post/...).
            passed = 0
            for w in self.scenario.stage2.windows:
                wy = float(w.center_global_xy[1])
                for sg in segs:
                    end_xyz = sg["end_xyz"]
                    if abs(float(end_xyz[1]) - wy) < 0.05:
                        end_s = float(sg["start_s"]) + float(sg["length"])  # type: ignore[arg-type]
                        if s_arc >= end_s - 0.05:
                            passed += 1
                        break
            metrics.windows_passed = passed

        # Axis 1: Success (dual threshold)
        cr = metrics.completion_ratio if metrics.completion_ratio is not None else 0.0
        recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
        no_obstacle = (obstacle_collision_ticks == 0)
        metrics.success_strict = bool(
            cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_collision_ticks == 0
        )
        metrics.success_physical = bool(
            cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_ticks_physical == 0
        )
        metrics.success = metrics.success_strict  # back-compat
        metrics.success_criterion = (
            "completion_ratio>=0.95 AND reconfigurability==1.0 AND "
            "wall_hit_ticks==0 AND pair_collision_ticks==0 "
            "(@0.30m strict / @0.15m physical)"
        )

        self.metrics[stage_name] = metrics
        _dbg(
            "mission_centralised.py:run_stage2_streaming",
            "stage2 streaming done",
            {
                "ticks": tick_count,
                "elapsed_s": round(time.time() - metrics.start_time, 2),
                "success": metrics.success,
                "success_criterion": metrics.success_criterion,
                "duration_sec": round(metrics.duration_sec, 2),
                "formations_planned": metrics.formations_planned,
                "formations_visited": metrics.formations_visited,
                "reconfigurability": metrics.reconfigurability,
                "num_pair_collision_ticks": metrics.num_pair_collision_ticks,
                "min_pair_distance_m": metrics.min_pair_distance_m,
                "num_obstacle_collision_ticks": metrics.num_obstacle_collision_ticks,
                "leader_path_length_m": round(metrics.leader_path_length_m or 0.0, 3),
                "ideal_path_length_m": round(metrics.ideal_path_length_m or 0.0, 3),
                "path_efficiency": metrics.path_efficiency,
                "formation_rmse_mean": metrics.formation_rmse_mean,
                "completion_ratio": metrics.completion_ratio,
                "formation_switches": metrics.formation_switches,
            },
            hypothesis_id="H12",
        )
        return metrics

    def run_stage3(self) -> StageMetrics:
        """Stage3 entry point. Defer to the streaming variant (same smooth
        motion path that stage1/2/4 use). If PositionMotion cannot be
        loaded, ``run_stage3_streaming`` falls back to ``run_stage3_legacy``.
        """
        return self.run_stage3_streaming()

    def run_stage3_legacy(self) -> StageMetrics:
        """Legacy discrete-waypoint Stage3 (behavior-action go_to). Kept as
        fallback only; motion is jerky compared to the streaming variant.
        """
        static_obstacles: List[Tuple[float, float, float]] = []
        if self.scenario.stage3:
            safe_radius = self.scenario.stage3.obstacle_diameter / 2.0 + 0.2
            static_obstacles = [(x, y, safe_radius) for x, y in self.scenario.stage3.obstacles_global]
        return self.run_plan(
            "stage3",
            self.planner.plan_stage3(),
            speed=0.35,
            transition_points=2,
            static_obstacles=static_obstacles,
        )

    def run_stage3_streaming(self) -> StageMetrics:
        """Stage3: continuous 10 Hz streaming through the forest of trees.

        Mirrors ``run_stage2_streaming`` with two differences:
          * Obstacle model is cylindrical (trees), not planar (walls).
          * Stage-specific fairness metric is ``detour_ratio`` (actual leader
            path length / Euclidean distance from start to end).

        Phases (identical to stage2):
          0. Prime PositionMotion at current drone XY, cruise_z.
          1. Rally-in-place in the plan's initial formation (``square``)
             with greedy nearest-neighbour slot assignment.
          2. Ferry from current leader XY to ``rally_in`` waypoint.
          3. Streaming cruise along the planner's A*-smoothed polyline at a
             fixed linear speed, formation per segment.
          4. Hover at the final pose for 1 s.
        """
        stage_name = "stage3"
        stage = self.scenario.stage3
        metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
        metrics.start_time = time.time()
        if stage is None:
            metrics.end_time = time.time()
            self.metrics[stage_name] = metrics
            return metrics

        if not self.dry_run:
            if not self._ensure_motion_ref_loaded():
                self._log("[stage3] motion_ref handler load failed; falling back to legacy go_to plan")
                return self.run_stage3_legacy()

        plan = self.planner.plan_stage3()
        if not plan.waypoints:
            self._log("[stage3] empty plan; falling back to legacy")
            return self.run_stage3_legacy()

        cruise_z = self.planner.cruise_height
        default_spacing = self.controller.default_spacing

        rate_hz = 10.0
        dt = 1.0 / rate_hz
        target_speed = 0.35   # leader linear speed along polyline (m/s)
        slew_speed = 0.40     # m/s during ferry
        rally_duration = 2.5
        hover_duration = 1.0
        twist_slew = [0.6, 0.6, 0.5]
        twist_cruise = [0.6, 0.6, 0.5]
        twist_rally = [0.4, 0.4, 0.3]
        PAIR_SAFETY_M = 0.30
        PAIR_PHYSICAL_M = 0.15
        DRONE_RADIUS = 0.12

        from centralised.formations import get_formation_offsets as _get_offsets
        from centralised.utils import rotate_xy as _rotate_xy

        # ---- Leader current position (used for rally / ferry start)
        try:
            if not self.dry_run:
                lp = self.drones[0].position
                lp_x, lp_y = float(lp[0]), float(lp[1])
            else:
                lp_x, lp_y = 0.0, 0.0
        except Exception:
            lp_x, lp_y = 0.0, 0.0

        entry = plan.waypoints[0]
        entry_x, entry_y, entry_z = entry
        if math.hypot(entry_x - lp_x, entry_y - lp_y) > 0.05:
            slew_yaw = math.atan2(entry_y - lp_y, entry_x - lp_x)
        else:
            slew_yaw = 0.0

        # ---- Greedy slot assignment for the initial (square) rally
        initial_form = plan.formation_names[0].lower() if plan.formation_names else "square"
        rally_offsets_local = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
        rally_slots_global_xy: List[Tuple[float, float]] = []
        for off in rally_offsets_local:
            dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
            rally_slots_global_xy.append((lp_x + dxg, lp_y + dyg))

        follower_positions_now: List[Tuple[float, float]] = []
        if not self.dry_run:
            for d in self.drones[1:]:
                try:
                    p = d.position
                    follower_positions_now.append((float(p[0]), float(p[1])))
                except Exception:
                    follower_positions_now.append((lp_x, lp_y))
        else:
            follower_positions_now = [(lp_x + i * 0.3, lp_y) for i in range(4)]

        slot_to_follower = _greedy_slot_assignment(follower_positions_now, rally_slots_global_xy)
        drone_to_slot: List[int] = [0] * 4
        for slot_i, drone_idx in enumerate(slot_to_follower):
            if 0 <= drone_idx < 4:
                drone_to_slot[drone_idx] = slot_i

        # ---- Build polyline segments from plan.waypoints[0..N-1]
        segs: List[Dict[str, object]] = []
        total_len = 0.0
        for i in range(1, len(plan.waypoints)):
            a = plan.waypoints[i - 1]
            b = plan.waypoints[i]
            seg_len = math.hypot(b[0] - a[0], b[1] - a[1]) + 0.5 * abs(b[2] - a[2])
            fname = (
                plan.formation_names[i].lower()
                if i < len(plan.formation_names)
                else plan.formation_names[-1].lower()
            )
            segs.append({
                "start_xyz": a,
                "end_xyz": b,
                "length": seg_len,
                "start_s": total_len,
                "formation": fname,
            })
            total_len += seg_len
        if total_len < 1e-6 or not segs:
            self._log("[stage3] zero-length polyline; falling back to legacy")
            return self.run_stage3_legacy()

        formations_planned_set = {n.lower() for n in plan.formation_names}
        formations_visited_set: set = set()

        # ---- Tree (cylindrical) obstacle model for stage3 collision detection.
        # A drone is considered to have HIT a tree if its XY distance to the
        # tree centre is less than (tree_radius + drone_radius) and it is
        # below the tree's height. At cruise_z = 1.2 m the drones are always
        # below the 5 m tree height so we only need the XY check.
        tree_radius = stage.obstacle_diameter / 2.0
        trees: List[Tuple[float, float]] = list(stage.obstacles_global)

        def _tree_hit(px: float, py: float, pz: float) -> bool:
            """True iff this drone XY is inside any tree's body footprint."""
            if pz > stage.obstacle_height + 0.1:
                return False
            for (tx, ty) in trees:
                if math.hypot(px - tx, py - ty) < (tree_radius + DRONE_RADIUS):
                    return True
            return False

        # ---- Telemetry buffers
        position_err_sqsum = 0.0
        position_err_samples = 0
        min_pair_dist_seen = float("inf")
        pair_collision_ticks = 0
        pair_ticks_physical = 0
        obstacle_collision_ticks = 0
        leader_path_len = 0.0
        last_leader_xy: Optional[Tuple[float, float]] = None
        formation_switch_count = 0
        tick_count = 0

        final_leader: Vec3 = entry
        final_followers_indexed: List[Vec3] = [(lp_x, lp_y, cruise_z) for _ in range(4)]

        def _interp_polyline(s_arc: float) -> Tuple[Vec3, str, int]:
            """Map arclength ``s_arc`` (m) to (leader_xyz, formation, seg_idx)."""
            if s_arc <= 0.0:
                sg0 = segs[0]
                return (tuple(sg0["start_xyz"]), str(sg0["formation"]), 0)  # type: ignore[return-value]
            for i, sg in enumerate(segs):
                start_s = float(sg["start_s"])  # type: ignore[arg-type]
                length = float(sg["length"])    # type: ignore[arg-type]
                if s_arc < start_s + length:
                    seg_t = (s_arc - start_s) / max(1e-6, length)
                    a = sg["start_xyz"]
                    b = sg["end_xyz"]
                    x = a[0] + (b[0] - a[0]) * seg_t  # type: ignore[index]
                    y = a[1] + (b[1] - a[1]) * seg_t  # type: ignore[index]
                    z = a[2] + (b[2] - a[2]) * seg_t  # type: ignore[index]
                    return ((x, y, z), str(sg["formation"]), i)
            last = segs[-1]
            return (tuple(last["end_xyz"]), str(last["formation"]), len(segs) - 1)  # type: ignore[return-value]

        def _segment_yaw(seg_idx: int) -> float:
            """Yaw along segment seg_idx. Fall back to nearest valid segment."""
            for j in range(seg_idx, -1, -1):
                sg = segs[j]
                a = sg["start_xyz"]; b = sg["end_xyz"]  # type: ignore[assignment]
                dx = b[0] - a[0]; dy = b[1] - a[1]      # type: ignore[index]
                if math.hypot(dx, dy) >= 0.2:
                    return math.atan2(dy, dx)
            return 0.0

        def _publish_swarm(leader_tgt: Vec3, slots_in_slot_order: List[Vec3],
                           yaw_cmd: float, twist: List[float]) -> None:
            if self.dry_run:
                return
            self._stream_position(self.drones[0], leader_tgt, yaw_cmd, twist)
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if 0 <= slot_i < len(slots_in_slot_order):
                    ft = slots_in_slot_order[slot_i]
                    self._stream_position(self.drones[1 + follower_idx], ft, yaw_cmd, twist)

        def _sample_telemetry(commanded_slots: List[Vec3]) -> None:
            nonlocal position_err_sqsum, position_err_samples
            nonlocal min_pair_dist_seen, pair_collision_ticks, pair_ticks_physical
            nonlocal obstacle_collision_ticks
            if self.dry_run:
                return
            for follower_idx in range(min(4, len(self.drones) - 1)):
                slot_i = drone_to_slot[follower_idx]
                if not (0 <= slot_i < len(commanded_slots)):
                    continue
                tgt = commanded_slots[slot_i]
                try:
                    p = self.drones[1 + follower_idx].position
                    err2 = (float(p[0]) - tgt[0]) ** 2 + (float(p[1]) - tgt[1]) ** 2
                    position_err_sqsum += err2
                    position_err_samples += 1
                except Exception:
                    pass
            tick_min_pair = float("inf")
            any_tree_hit = False
            try:
                swarm_xyz: List[Tuple[float, float, float]] = []
                for d in self.drones:
                    p = d.position
                    swarm_xyz.append((float(p[0]), float(p[1]), float(p[2])))
                for i in range(len(swarm_xyz)):
                    for j in range(i + 1, len(swarm_xyz)):
                        d_pair = math.hypot(
                            swarm_xyz[i][0] - swarm_xyz[j][0],
                            swarm_xyz[i][1] - swarm_xyz[j][1],
                        )
                        if d_pair < tick_min_pair:
                            tick_min_pair = d_pair
                    if _tree_hit(*swarm_xyz[i]):
                        any_tree_hit = True
            except Exception:
                pass
            if tick_min_pair < min_pair_dist_seen:
                min_pair_dist_seen = tick_min_pair
            if tick_min_pair < PAIR_SAFETY_M:
                pair_collision_ticks += 1
            if tick_min_pair < PAIR_PHYSICAL_M:
                pair_ticks_physical += 1
            if any_tree_hit:
                obstacle_collision_ticks += 1

        _dbg(
            "mission_centralised.py:run_stage3_streaming",
            "plan",
            {
                "num_waypoints": len(plan.waypoints),
                "num_segments": len(segs),
                "total_len_m": round(total_len, 2),
                "target_speed_m_s": target_speed,
                "T_est_s": round(total_len / target_speed, 1),
                "formations_planned": sorted(formations_planned_set),
                "trees": len(trees),
                "entry": [round(entry_x, 3), round(entry_y, 3), round(entry_z, 3)],
                "slot_to_follower_idx": slot_to_follower,
                "drone_to_slot": drone_to_slot,
            },
            hypothesis_id="H12",
        )

        # ---- Phase 0: prime pipeline at current drone XY, cruise z.
        if not self.dry_run:
            for drone in self.drones:
                try:
                    cur = drone.position
                    px = float(cur[0]); py = float(cur[1])
                except Exception:
                    px, py = lp_x, lp_y
                self._stream_position(drone, (px, py, cruise_z), slew_yaw, twist_slew)
            time.sleep(0.5)

        # ---- Phase 1: Rally in place (leader holds, followers -> columnn).
        # The initial formation is ``columnn`` aligned with the ferry
        # direction, so followers line up behind the leader along the
        # eventual direction of travel -- no wide lateral spread that
        # could clip trees at the forest entry.
        if not self.dry_run and rally_duration > 0.0:
            rally_slots_xyz: List[Vec3] = [(p[0], p[1], cruise_z) for p in rally_slots_global_xy]
            t0 = time.time()
            while time.time() - t0 < rally_duration:
                _publish_swarm((lp_x, lp_y, cruise_z), rally_slots_xyz, slew_yaw, twist_rally)
                _sample_telemetry(rally_slots_xyz)
                tick_count += 1
                final_leader = (lp_x, lp_y, cruise_z)
                final_followers_indexed = rally_slots_xyz
                time.sleep(dt)
            self._log(f"[stage3] Rally complete, formed {initial_form.upper()} at leader position")

        # ---- Phase 2: Ferry from leader's current XY to entry waypoint.
        slew_dist = math.hypot(entry_x - lp_x, entry_y - lp_y) + abs(entry_z - cruise_z)
        if slew_dist > 0.05:
            T_slew = max(1.5, slew_dist / slew_speed)
            t0 = time.time()
            while True:
                e = time.time() - t0
                if e >= T_slew:
                    break
                frac = e / T_slew
                lx = lp_x + (entry_x - lp_x) * frac
                ly = lp_y + (entry_y - lp_y) * frac
                lz = cruise_z + (entry_z - cruise_z) * frac
                leader_target: Vec3 = (lx, ly, lz)
                offs = _get_offsets(initial_form, spacing=default_spacing, z=0.0)
                slot_targets: List[Vec3] = []
                for off in offs:
                    dxg, dyg = _rotate_xy(off[0], off[1], slew_yaw)
                    slot_targets.append((lx + dxg, ly + dyg, lz + off[2]))
                _publish_swarm(leader_target, slot_targets, slew_yaw, twist_slew)
                _sample_telemetry(slot_targets)
                tick_count += 1
                final_leader = leader_target
                final_followers_indexed = slot_targets
                time.sleep(dt)
            self._log(f"[stage3] Ferry complete, leader at entry ({entry_x:.2f}, {entry_y:.2f}, {entry_z:.2f})")

        # ---- Phase 3: Streaming cruise along the planned polyline.
        # Stage3 is ALL columnn, so every follower just trail-follows the
        # leader's A* path at a fixed arclength offset. No formation blends,
        # no rigid-body rotation, no slot crossings.
        previous_formation = initial_form
        formations_visited_set.add(initial_form)
        s_arc = 0.0
        last_log = 0.0
        t_start = time.time()
        T_max = (total_len / target_speed) * 2.0 + 20.0
        spacing_col = min(default_spacing, 0.5)  # matches column_n()
        while True:
            t_rel = time.time() - t_start
            if t_rel > T_max:
                self._log(f"[stage3] cruise timed out at t={t_rel:.1f}s, s={s_arc:.2f}/{total_len:.2f}")
                break
            s_arc += target_speed * dt
            leader_target, formation, seg_idx = _interp_polyline(s_arc)
            lx, ly, lz = leader_target
            yaw = _segment_yaw(seg_idx)

            # Trail-following for columnn: follower k targets the leader's
            # polyline at arclength (s_arc - (k+1)*spacing). A* guarantees
            # the leader path clears every tree, so trailing followers do
            # too.
            slot_targets: List[Vec3] = []
            for k in range(4):
                s_k = s_arc - (k + 1) * spacing_col
                if s_k <= 0.0:
                    dxg, dyg = _rotate_xy(-(k + 1) * spacing_col, 0.0, yaw)
                    slot_targets.append((lx + dxg, ly + dyg, lz))
                else:
                    tgt, _, _ = _interp_polyline(s_k)
                    slot_targets.append(tgt)

            _publish_swarm(leader_target, slot_targets, yaw, twist_cruise)
            _sample_telemetry(slot_targets)

            if not self.dry_run:
                try:
                    p = self.drones[0].position
                    axy = (float(p[0]), float(p[1]))
                    if last_leader_xy is not None:
                        leader_path_len += math.hypot(axy[0] - last_leader_xy[0], axy[1] - last_leader_xy[1])
                    last_leader_xy = axy
                except Exception:
                    pass

            tick_count += 1
            metrics.completed_waypoints = tick_count
            final_leader = leader_target
            final_followers_indexed = slot_targets

            if formation != previous_formation:
                self._log(f"[stage3] FORMATION -> {formation.upper()}")
                formation_switch_count += 1
            previous_formation = formation
            formations_visited_set.add(formation)

            if time.time() - last_log >= 1.0:
                last_log = time.time()
                _dbg(
                    "mission_centralised.py:run_stage3_streaming",
                    "cruise tick",
                    {
                        "t_s": round(t_rel, 2),
                        "s_m": round(s_arc, 2),
                        "total_s_m": round(total_len, 2),
                        "seg_idx": seg_idx,
                        "leader_target": [round(lx, 3), round(ly, 3), round(lz, 3)],
                        "formation": formation,
                    },
                    hypothesis_id="H12",
                )

            if s_arc >= total_len:
                break
            time.sleep(dt)

        # ---- Phase 4: brief hover at final pose so the columnn is visible.
        hover_yaw = _segment_yaw(len(segs) - 1)
        hover_twist = [0.2, 0.2, 0.2]
        if not self.dry_run:
            t0 = time.time()
            while time.time() - t0 < hover_duration:
                _publish_swarm(final_leader, final_followers_indexed, hover_yaw, hover_twist)
                time.sleep(dt)

        # ---- Finalise metrics
        metrics.planned_waypoints = len(plan.waypoints)
        metrics.end_time = time.time()

        if position_err_samples > 0:
            metrics.formation_rmse_mean = math.sqrt(position_err_sqsum / position_err_samples)
        if min_pair_dist_seen < float("inf"):
            metrics.min_pair_distance_m = min_pair_dist_seen
        metrics.completion_ratio = max(0.0, min(1.0, s_arc / total_len))
        metrics.formation_switches = formation_switch_count

        # Axis 2: Reconfigurability
        metrics.formations_planned = len(formations_planned_set)
        metrics.formations_visited = len(formations_visited_set & formations_planned_set)

        # Axis 4: Collision avoidance
        metrics.num_pair_collision_ticks = pair_collision_ticks  # 0.30 m strict
        metrics.num_pair_ticks_physical = pair_ticks_physical    # 0.15 m physical
        metrics.num_obstacle_collision_ticks = obstacle_collision_ticks

        # Axis 5: Efficiency
        metrics.leader_path_length_m = leader_path_len
        metrics.ideal_path_length_m = total_len

        # Time-taken breakdown
        metrics.cruise_time_s = total_len / target_speed if target_speed > 1e-6 else None
        tpf: Dict[str, float] = {}
        for sg in segs:
            name = str(sg["formation"])
            seg_dur = float(sg["length"]) / target_speed if target_speed > 1e-6 else 0.0  # type: ignore[arg-type]
            tpf[name] = tpf.get(name, 0.0) + seg_dur
        metrics.time_per_formation = tpf

        # Stage3-specific fairness metric: detour_ratio = actual leader path
        # length / euclidean distance from start to end. 1.0 = straight line;
        # larger = more obstacle-avoidance detour. Use the PLANNED polyline
        # length (total_len) rather than the measured ``leader_path_len`` so
        # the metric reflects the planner's plan, not tracking noise.
        sx, sy = stage.start_point_global
        ex, ey = stage.end_point_global
        straight = math.hypot(ex - sx, ey - sy)
        if straight > 1e-6:
            metrics.detour_ratio = total_len / straight

        # Axis 1: Success (dual threshold)
        cr = metrics.completion_ratio if metrics.completion_ratio is not None else 0.0
        recfg = metrics.reconfigurability if metrics.reconfigurability is not None else 0.0
        no_obstacle = (obstacle_collision_ticks == 0)
        metrics.success_strict = bool(
            cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_collision_ticks == 0
        )
        metrics.success_physical = bool(
            cr >= 0.95 and recfg >= 0.999 and no_obstacle and pair_ticks_physical == 0
        )
        metrics.success = metrics.success_strict  # back-compat
        metrics.success_criterion = (
            "completion_ratio>=0.95 AND reconfigurability==1.0 AND "
            "tree_hit_ticks==0 AND pair_collision_ticks==0 "
            "(@0.30m strict / @0.15m physical)"
        )

        self.metrics[stage_name] = metrics
        _dbg(
            "mission_centralised.py:run_stage3_streaming",
            "stage3 streaming done",
            {
                "ticks": tick_count,
                "elapsed_s": round(time.time() - metrics.start_time, 2),
                "success": metrics.success,
                "success_criterion": metrics.success_criterion,
                "duration_sec": round(metrics.duration_sec, 2),
                "formations_planned": metrics.formations_planned,
                "formations_visited": metrics.formations_visited,
                "reconfigurability": metrics.reconfigurability,
                "num_pair_collision_ticks": metrics.num_pair_collision_ticks,
                "min_pair_distance_m": metrics.min_pair_distance_m,
                "num_obstacle_collision_ticks": metrics.num_obstacle_collision_ticks,
                "leader_path_length_m": round(metrics.leader_path_length_m or 0.0, 3),
                "ideal_path_length_m": round(metrics.ideal_path_length_m or 0.0, 3),
                "path_efficiency": metrics.path_efficiency,
                "formation_rmse_mean": metrics.formation_rmse_mean,
                "completion_ratio": metrics.completion_ratio,
                "formation_switches": metrics.formation_switches,
                "detour_ratio": metrics.detour_ratio,
            },
            hypothesis_id="H12",
        )
        return metrics

    def _ensure_motion_ref_loaded(self) -> bool:
        """Lazily attach a ``PositionMotion`` handler to every drone (stage4 only).

        We do NOT use ``MotionReferenceHandlerModule`` because its constructor
        does a 3 s blocking ``wait_for_service`` on a non-existent
        ``traj_gen/run_node`` service — across 5 drones that is a 15 s stall
        every time stage4 starts. Instead we instantiate ``PositionMotion``
        directly, which only registers the ``motion_reference/pose`` +
        ``motion_reference/twist`` publishers and the controller-info
        subscription.
        """
        if self.dry_run:
            return True
        try:
            from as2_motion_reference_handlers.position_motion import PositionMotion
        except Exception as exc:
            _dbg(
                "mission_centralised.py:_ensure_motion_ref_loaded",
                "PositionMotion import failed",
                {"error": repr(exc)},
                hypothesis_id="H7",
            )
            return False
        for drone in self.drones:
            if hasattr(drone, "_position_motion") and drone._position_motion is not None:
                continue
            try:
                drone._position_motion = PositionMotion(drone)
            except Exception as exc:
                _dbg(
                    "mission_centralised.py:_ensure_motion_ref_loaded",
                    "PositionMotion init failed",
                    {"ns": drone.namespace, "error": repr(exc)},
                    hypothesis_id="H7",
                )
                return False
        return True

    def _stream_position(
        self,
        drone: "MissionDrone",
        pose_xyz: Vec3,
        yaw_rad: float,
        twist_limit: Sequence[float],
    ) -> bool:
        """Publish a single pose reference via the lazy-loaded PositionMotion handler."""
        if self.dry_run:
            return True
        handler = getattr(drone, "_position_motion", None)
        if handler is None:
            return False
        try:
            return handler.send_position_command_with_yaw_angle(
                pose=[float(pose_xyz[0]), float(pose_xyz[1]), float(pose_xyz[2])],
                twist_limit=list(float(v) for v in twist_limit),
                pose_frame_id="earth",
                twist_frame_id="earth",
                yaw_angle=float(yaw_rad),
            )
        except Exception as exc:
            _dbg(
                "mission_centralised.py:_stream_position",
                "pose stream failed",
                {"ns": getattr(drone, "namespace", "?"), "error": repr(exc)},
                hypothesis_id="H6",
            )
            return False

    def run_stage4(self) -> StageMetrics:
        """Stage4: real-time reactive avoidance with continuous 10 Hz streaming.

        Architecture:
          Phase 1 -- Rally (blocking go_to):
              Move all 5 drones to ``diamond`` formation at ``start_point``.
          Phase 2 -- Obstacle warmup:
              Wait up to 4 s for the DynamicObstacleMonitor to populate all
              ``num_obstacles`` tracks from the ``/dynamic_obstacles/locations``
              topic (``geometry_msgs/Pose``).
          Phase 3 -- Reactive cruise (10 Hz stream):
              The leader's NOMINAL target progresses linearly from start to
              end at ``cruise_speed``. Every 100 ms we:
                (a) snapshot the obstacle field;
                (b) shift the leader target **laterally** w.r.t. the start→end
                    corridor (plus a lateral-only emergency term when very
                    close), not along a short local segment whose normal flips;
                (c) pick formation: ``columnn`` if any obstacle within
                    ``compress_trigger``, else ``diamond``;
                (d) publish pose references to all drones using
                    ``motion_ref_handler.position`` (goes straight into the
                    motion controller, no action round-trip).
          Phase 4 -- Hover:
              Once the leader reaches end_point, send hover to stabilise.
        """
        from centralised.obstacle_avoidance import avoid_dynamic_obstacles
        stage_name = "stage4"
        stage = self.scenario.stage4
        metrics = StageMetrics(stage=stage_name, planned_waypoints=0)
        metrics.start_time = time.time()
        if stage is None:
            metrics.end_time = time.time()
            self.metrics[stage_name] = metrics
            return metrics

        _dbg(
            "mission_centralised.py:run_stage4",
            "run_stage4 body start",
            {"dry_run": self.dry_run, "num_drones": len(self.drones)},
            hypothesis_id="H4",
        )

        # Cruise altitude: 1.8 m instead of 1.2 m. At 1.2 m the diamond
        # formation sat in the propeller-downwash interference band and
        # drone0 lost altitude mid-cruise (run-1 debug.log: z fell from
        # 1.17 m -> 0.005 m in 1.5 s). 1.8 m is still well below the 5 m
        # obstacle height so the drones must still avoid laterally.
        z = 1.8
        start_x, start_y = stage.start_point_global
        end_x, end_y = stage.end_point_global
        total_dist = math.hypot(end_x - start_x, end_y - start_y)
        cruise_speed = 0.8  # m/s, leader nominal forward speed
        nominal_yaw = math.atan2(end_y - start_y, end_x - start_x)

        # Avoidance radii. Earlier values (safe_dist=0.85 m,
        # compress_trigger=1.75 m) meant run-1 never triggered any shift
        # or formation switch because the nearest obstacle only ever got
        # to 2.4 m. For a visible + actually-safety-providing reaction we
        # widen both significantly. Obstacles are 0.5 m diameter, so with
        # a 2.5 m lateral deflection radius the swarm starts curving
        # around obstacles from ~3 m away.
        leader_radius = 0.3
        safe_dist = (stage.obstacle_diameter / 2.0) + leader_radius + 2.0
        # Formation-compress trigger: if any obstacle lies within this
        # radius of the NEXT 5 m of planned path, switch to columnn so
        # followers file in behind the leader before it reaches the gap.
        compress_trigger = 3.5
        # How far ahead of the leader we test for "obstacle on path".
        forward_lookahead_m = 5.0
        # Swarm lateral spacing for diamond. 1.0 m x 1.0 m footprint gives
        # clearly visible formation vs the 0 m columnn footprint.
        swarm_spacing = 1.0

        # ---------------- Phase 1: Rally (SKIPPED) ----------------
        # Previously we ran `go_to_async` for each drone to rally into diamond
        # at start_point. Runtime log (run-3) showed that sequential dispatch
        # takes ~22 s per drone for first-time goal acceptance (total ~110 s),
        # and during that window aerostack2 has NO active setpoint on the
        # drones, so they free-fall in Gazebo. drone1 and drone4 both ended up
        # at z < 0.05 m before we ever reached the streaming cruise phase.
        #
        # With start_point now aligned with the drone spawn cluster (scenario
        # YAML: start_point = (0, -6) rel -> (0, 0) abs == spawn center), the
        # rally physically achieves ~nothing anyway: each drone only needs to
        # move <= 2 m to its diamond slot. We therefore skip rally entirely
        # and hand off straight from takeoff to the Phase-3 streaming loop,
        # which starts issuing setpoints within ~1 s and prevents the
        # free-fall.
        rally_leader: Vec3 = (start_x, start_y, z)
        rally_followers = self.controller.compute_targets(
            leader_position=rally_leader,
            leader_yaw=nominal_yaw,
            formation_name="diamond",
            previous_formation_name="diamond",
            transition_alpha=1.0,
            static_obstacles=None,
        )
        _dbg(
            "mission_centralised.py:run_stage4",
            "phase1 rally SKIPPED (streaming takes over from takeoff)",
            {
                "leader_target_would_be": list(rally_leader),
                "yaw_deg": round(math.degrees(nominal_yaw), 1),
                "follower_slots_would_be": [list(t) for t in rally_followers],
            },
            hypothesis_id="H8",
        )

        # ---------------- Phase 2: Obstacle warmup ----------------
        if self.obstacle_monitor is not None and not self.dry_run:
            t0 = time.time()
            while time.time() - t0 < 4.0 and self.obstacle_monitor.count() < stage.num_obstacles:
                time.sleep(0.15)
            stats = self.obstacle_monitor.stats()
            _dbg(
                "mission_centralised.py:run_stage4",
                "phase2 warmup done",
                {
                    "count": self.obstacle_monitor.count(),
                    "expected": stage.num_obstacles,
                    "msg_count": stats.get("msg_count"),
                    "elapsed_s": round(time.time() - t0, 2),
                },
                hypothesis_id="H6",
            )

        # ---------------- Phase 3: Reactive 10 Hz cruise ----------------
        if not self.dry_run:
            # Lazy-load motion_reference_handler only NOW (after arm/takeoff/rally
            # are finished), to avoid the DDS-flood described above.
            if not self._ensure_motion_ref_loaded():
                _dbg(
                    "mission_centralised.py:run_stage4",
                    "_ensure_motion_ref_loaded FAILED",
                    {"ns": [d.namespace for d in self.drones]},
                    hypothesis_id="H4",
                )
                self._log("[stage4] motion_ref_handler load failed; aborting reactive cruise")
                metrics.end_time = time.time()
                self.metrics[stage_name] = metrics
                return metrics
            _dbg(
                "mission_centralised.py:run_stage4",
                "motion_ref_handler loaded on all drones",
                {"ns": [d.namespace for d in self.drones]},
                hypothesis_id="H7",
            )
            # Prime the motion reference pipeline with each drone's CURRENT
            # position (hover in place). Since we skipped rally, drones are
            # still near their spawn points at z ~= 1.2; priming with the
            # old rally targets would cause an immediate horizontal jump on
            # the first set-point. Holding position first, then letting the
            # cruise loop slew the set-point, gives smooth behaviour.
            for drone in self.drones:
                try:
                    cur = drone.position
                    cx = float(cur[0]); cy = float(cur[1]); cz = float(cur[2])
                except Exception:
                    cx, cy, cz = 0.0, 0.0, z
                # Prime at cruise_height z (=1.8 m here) so the drones
                # immediately start climbing to cruise altitude rather
                # than hovering at takeoff altitude (~1.2 m) then jumping.
                cz_cmd = z
                self._stream_position(drone, (cx, cy, cz_cmd), nominal_yaw, [1.2, 1.2, 0.6])
            # #region agent log
            _dbg(
                "mission_centralised.py:run_stage4",
                "priming streamed at drone current positions",
                {
                    "positions": [
                        [round(float(d.position[0]), 3), round(float(d.position[1]), 3), round(float(d.position[2]), 3)]
                        for d in self.drones
                    ],
                    "yaw_deg": round(math.degrees(nominal_yaw), 1),
                },
                hypothesis_id="H8",
            )
            # #endregion
            time.sleep(0.8)

        rate_hz = 10.0
        dt_sleep = 1.0 / rate_hz
        # Hard ceiling: travel at ~0.6 m/s (natural consequence of 1.5 m
        # lookahead and twist limit of 0.6 m/s) should clear total_dist in
        # total_dist/0.6 s; give it 2x that as a safety window.
        T_max = max(25.0, (total_dist / 0.6) * 2.0 + 10.0)
        # Horizontal twist 0.6 m/s keeps swarm speed safe. Vertical
        # twist raised to 0.6 m/s (was 0.3 m/s) because run-2 showed
        # drone0 bleeding altitude at 0.5 m/s but the 0.3 m/s climb
        # cap couldn't keep up, letting z collapse from 1.17 m -> 0 m.
        twist_limit = [0.6, 0.6, 0.6]
        # Pure-pursuit lookahead increased 1.5 m -> 2.5 m. With 1.5 m
        # and safe_dist=2.5 m, the pursuit waypoint was too close to
        # the drone: when we finally saw a threat (obs4 at (-0.77, 10.57)
        # on the leader's northbound path) the drone was already <1 m
        # away physically, which is past the reaction distance of the
        # platform's PID. 2.5 m buys us ~4 s of reaction time at 0.6 m/s
        # cruise while still keeping speed tightly coupled to the leader.
        lookahead_m = 2.5

        previous_formation = "diamond"
        t_start = time.time()
        last_log = 0.0
        tick_count = 0
        final_reached = False
        last_leader_target: Optional[Vec3] = None
        # Avoidance shift is low-pass filtered tick-to-tick. The raw output
        # of avoid_dynamic_obstacles() can jump by 2+ m in one tick when
        # the set of "threatening" obstacles flips (one moved past, another
        # entered). Feeding that raw output to the platform makes the leader
        # zig-zag, loses altitude (seen in debug.log: z collapsed from 1.84 m
        # to 0.80 m while shift_y oscillated between 0.01 and 2.41 m), and
        # makes avoidance look like "drunken wandering" rather than
        # deliberate obstacle dodging. alpha=0.35 blends 35% new / 65% old.
        shift_dx_f = 0.0
        shift_dy_f = 0.0
        # MAX_SHIFT >= min_d (=2.8 m) so shift can fully clear danger zone.
        MAX_SHIFT = 3.0
        # EMA at 10 Hz: 0.5 -> ~0.15 s response vs 0.22 -> ~0.4 s.
        SHIFT_ALPHA = 0.5
        # Airborne filter for avoidance: drones below this z are skipped when
        # computing swarm-wide shift (a grounded corpse shouldn't steer avoidance).
        AVOID_Z_MIN = 0.4
        # Altitude recovery counter -- number of consecutive ticks with
        # z < z_min_safe. When this exceeds PAUSE_TICKS we temporarily freeze
        # horizontal chase and prioritize climb.
        z_min_safe = 1.3
        low_z_ticks = 0
        PAUSE_TICKS = 2
        # Visible-event logging helpers: rate-limit "AVOIDANCE ENGAGED"
        # and "FORMATION SWITCHED" stdout prints so the user can see them
        # on the terminal without spam.
        last_avoid_print = 0.0
        # Formation hysteresis: require N consecutive ticks below / above
        # compress_trigger before actually switching, so random obstacle
        # noise can't whiplash the formation every tick.
        hysteresis_counter = 0
        hysteresis_ticks = 5

        while True:
            t = time.time() - t_start
            if t > T_max:
                _dbg(
                    "mission_centralised.py:run_stage4",
                    "cruise timed out",
                    {"elapsed_s": round(t, 2), "T_max": round(T_max, 2)},
                    hypothesis_id="H6",
                )
                break

            # --- Pure-pursuit leader target from ACTUAL leader position. ---
            try:
                pos = self.drones[0].position if not self.dry_run else (start_x, start_y, z)
            except Exception:
                pos = (start_x, start_y, z)
            lp_x = float(pos[0]) if pos is not None else start_x
            lp_y = float(pos[1]) if pos is not None else start_y
            # Vector from actual leader to end_point.
            dx_end = end_x - lp_x
            dy_end = end_y - lp_y
            dist_remaining = math.hypot(dx_end, dy_end)
            if dist_remaining < 1e-3:
                base_x, base_y = end_x, end_y
            else:
                step = min(lookahead_m, dist_remaining)
                base_x = lp_x + step * (dx_end / dist_remaining)
                base_y = lp_y + step * (dy_end / dist_remaining)

            # --- Swarm XY samples for merged avoidance (H2). ---
            swarm_xy: List[Tuple[float, float]] = [(lp_x, lp_y)]
            if not self.dry_run:
                try:
                    for d in self.drones[1:]:
                        p = d.position
                        if float(p[2]) < AVOID_Z_MIN:
                            continue
                        swarm_xy.append((float(p[0]), float(p[1])))
                except Exception:
                    pass

            # --- Reactive shift based on obstacle field. ---
            obs = self.obstacle_monitor.snapshot() if self.obstacle_monitor is not None else []
            # H3: slightly inflate clearance in the last metres — leader can be
            # past a pillar while a follower on the chain is still abeam it.
            safe_eff = safe_dist + (0.45 if dist_remaining < 3.5 else 0.0)
            # H2: avoidance used to use ONLY the leader XY. In columnn the
            # tail sits several metres along-track; moving pillars can be
            # closer to a follower than to the leader. Merge by taking the
            # lateral-shift vector with the largest magnitude among per-agent
            # avoid_dynamic_obstacles results (same base waypoint, same goal).
            raw_dx_merged = 0.0
            raw_dy_merged = 0.0
            raw_mag_best = 0.0
            for sx, sy in swarm_xy:
                rx, ry = avoid_dynamic_obstacles(
                    (sx, sy),
                    (base_x, base_y),
                    obs,
                    safe_distance=safe_eff,
                    lookahead_s=2.0,
                    goal=(end_x, end_y),
                )
                ddx = rx - base_x
                ddy = ry - base_y
                m = math.hypot(ddx, ddy)
                if m > raw_mag_best:
                    raw_mag_best = m
                    raw_dx_merged = ddx
                    raw_dy_merged = ddy
            raw_dx = raw_dx_merged
            raw_dy = raw_dy_merged
            # Clamp raw shift magnitude before filtering so one bad tick
            # can't dominate the filter state.
            raw_mag = math.hypot(raw_dx, raw_dy)
            if raw_mag > MAX_SHIFT:
                scale = MAX_SHIFT / raw_mag
                raw_dx *= scale
                raw_dy *= scale
            # EMA low-pass: filtered = alpha*new + (1-alpha)*old.
            shift_dx_f = SHIFT_ALPHA * raw_dx + (1.0 - SHIFT_ALPHA) * shift_dx_f
            shift_dy_f = SHIFT_ALPHA * raw_dy + (1.0 - SHIFT_ALPHA) * shift_dy_f
            # Re-clamp filtered value (belt & braces).
            f_mag = math.hypot(shift_dx_f, shift_dy_f)
            if f_mag > MAX_SHIFT:
                scale = MAX_SHIFT / f_mag
                shift_dx_f *= scale
                shift_dy_f *= scale
            lx = base_x + shift_dx_f
            ly = base_y + shift_dy_f

            # --- Altitude safety: latch XY-freeze on **leader z only**.
            # `min_z_swarm` (airborne-filtered) is still logged in reactive_tick
            # for diagnostics. Runtime evidence (term 21): followers often lag
            # 0.3–0.5 m below leader during climb; using min_z_swarm < z_min_safe
            # latched ALTITUDE RECOVERY almost continuously, which sets
            # lx,ly = lp (pause XY) and **removed visible lateral avoidance**
            # even while stdout still printed AVOIDANCE ENGAGED from shift state.
            _AIRBORNE_Z = 0.35  # m — below this = still on deck / not in cruise
            try:
                cur_lz = float(self.drones[0].position[2]) if not self.dry_run else z
            except Exception:
                cur_lz = z
            zs_all: List[float] = [cur_lz]
            if not self.dry_run:
                try:
                    for d in self.drones[1:]:
                        zs_all.append(float(d.position[2]))
                except Exception:
                    pass
            zs_air = [zz for zz in zs_all if zz >= _AIRBORNE_Z]
            min_z_swarm = min(zs_air) if zs_air else min(zs_all)
            if cur_lz < z_min_safe and not self.dry_run:
                low_z_ticks += 1
            else:
                low_z_ticks = 0
            if low_z_ticks > PAUSE_TICKS:
                # Hold XY at current leader position, only command full
                # climb to cruise altitude z. This bleeds the filter state
                # toward zero so resumed chase starts smooth.
                lx = lp_x
                ly = lp_y
                shift_dx_f *= 0.5
                shift_dy_f *= 0.5
            leader_target: Vec3 = (lx, ly, z)

            # Formation trigger is based on the distance from each obstacle
            # to the FORWARD PATH SEGMENT (leader -> leader + forward
            # lookahead), not just the current leader_target. This makes
            # the swarm switch to columnn BEFORE arriving at a gap.
            fwd_end_x = lp_x + forward_lookahead_m * (dx_end / max(dist_remaining, 1e-3))
            fwd_end_y = lp_y + forward_lookahead_m * (dy_end / max(dist_remaining, 1e-3))
            nearest = None
            nearest_to_leader = None
            if obs:
                def _seg_dist(o) -> float:
                    vx = fwd_end_x - lp_x
                    vy = fwd_end_y - lp_y
                    L2 = vx * vx + vy * vy
                    if L2 < 1e-6:
                        return math.hypot(o.x - lp_x, o.y - lp_y)
                    t = ((o.x - lp_x) * vx + (o.y - lp_y) * vy) / L2
                    t = max(0.0, min(1.0, t))
                    cx = lp_x + t * vx
                    cy = lp_y + t * vy
                    return math.hypot(o.x - cx, o.y - cy)
                nearest = min(_seg_dist(o) for o in obs)
                nearest_to_leader = min(math.hypot(lp_x - o.x, lp_y - o.y) for o in obs)

            # --- Formation with hysteresis. ---
            desired_compressed = nearest is not None and nearest < compress_trigger
            desired_expanded = nearest is None or nearest >= compress_trigger
            # H5: do not pop back to diamond in the last metres inside the
            # obstacle band — wide footprint late caused follower orbits into
            # pillars while leader was already near B.
            if desired_expanded and dist_remaining < 2.8:
                desired_expanded = False
            if desired_compressed and previous_formation != "columnn":
                hysteresis_counter = min(hysteresis_ticks, hysteresis_counter + 1)
                if hysteresis_counter >= hysteresis_ticks:
                    formation = "columnn"
                    hysteresis_counter = 0
                else:
                    formation = previous_formation
            elif desired_expanded and previous_formation != "diamond":
                hysteresis_counter = min(hysteresis_ticks, hysteresis_counter + 1)
                if hysteresis_counter >= hysteresis_ticks:
                    formation = "diamond"
                    hysteresis_counter = 0
                else:
                    formation = previous_formation
            else:
                hysteresis_counter = 0
                formation = previous_formation

            # --- Human-visible event log on stdout (rate-limited). ---
            now_s = time.time()
            if formation != previous_formation:
                self._log(
                    f"[stage4] FORMATION -> {formation.upper()} "
                    f"(nearest_seg={nearest:.2f} m)" if nearest is not None
                    else f"[stage4] FORMATION -> {formation.upper()}"
                )
            if f_mag > 0.25 and (now_s - last_avoid_print) > 1.0:
                nl = nearest_to_leader if nearest_to_leader is not None else float("inf")
                self._log(
                    f"[stage4] AVOIDANCE ENGAGED shift=({shift_dx_f:+.2f},{shift_dy_f:+.2f}) m | "
                    f"nearest_obs={nl:.2f} m | formation={formation}"
                )
                last_avoid_print = now_s
            if low_z_ticks > PAUSE_TICKS:
                if (now_s - last_avoid_print) > 0.8:
                    self._log(
                        f"[stage4] ALTITUDE RECOVERY leader_z={cur_lz:.2f} m "
                        f"(swarm_min_z={min_z_swarm:.2f}, thr={z_min_safe} m), pausing XY"
                    )
                    last_avoid_print = now_s

            follower_targets = self.controller.compute_targets(
                leader_position=leader_target,
                leader_yaw=nominal_yaw,
                formation_name=formation,
                spacing=swarm_spacing,
                previous_formation_name=previous_formation,
                transition_alpha=1.0,
                static_obstacles=None,
            )

            if not self.dry_run:
                self._stream_position(self.drones[0], leader_target, nominal_yaw, twist_limit)
                for i, ft in enumerate(follower_targets, start=1):
                    self._stream_position(self.drones[i], ft, nominal_yaw, twist_limit)

            tick_count += 1
            metrics.completed_waypoints = tick_count
            last_leader_target = leader_target

            if time.time() - last_log >= 0.5:
                last_log = time.time()
                follower_zs = []
                try:
                    for d in self.drones[1:]:
                        follower_zs.append(round(float(d.position[2]), 3))
                except Exception:
                    pass
                # #region agent log
                obs_m_leader: Optional[float] = None
                obs_m_swarm: Optional[float] = None
                if obs:
                    obs_m_leader = min(math.hypot(o.x - lp_x, o.y - lp_y) for o in obs)
                    obs_m_swarm = obs_m_leader
                    if not self.dry_run:
                        try:
                            for d in self.drones[1:]:
                                px = float(d.position[0])
                                py = float(d.position[1])
                                md = min(math.hypot(o.x - px, o.y - py) for o in obs)
                                obs_m_swarm = min(obs_m_swarm, md)
                        except Exception:
                            pass
                # #endregion
                _dbg(
                    "mission_centralised.py:run_stage4",
                    "reactive tick",
                    {
                        "t_s": round(t, 2),
                        "leader_pos": [round(lp_x, 3), round(lp_y, 3), round(float(pos[2]) if pos else z, 3)],
                        "follower_zs": follower_zs,
                        "min_z_swarm": round(min_z_swarm, 3),
                        "zs_airborne_n": len(zs_air),
                        "zs_grounded_n": len(zs_all) - len(zs_air),
                        "base": [round(base_x, 3), round(base_y, 3)],
                        "leader_target": [round(lx, 3), round(ly, 3)],
                        "shift_dx": round(lx - base_x, 3),
                        "shift_dy": round(ly - base_y, 3),
                        "merge_raw_mag": round(raw_mag_best, 3),
                        "safe_eff": round(safe_eff, 3),
                        "dist_remaining": round(dist_remaining, 3),
                        "obs_count": len(obs),
                        "obs": [[round(o.x, 2), round(o.y, 2), round(o.vx, 2), round(o.vy, 2)] for o in obs],
                        "nearest_seg": None if nearest is None else round(nearest, 3),
                        "nearest_pt": None if nearest_to_leader is None else round(nearest_to_leader, 3),
                        "obs_m_leader": None if obs_m_leader is None else round(obs_m_leader, 3),
                        "obs_m_swarm": None if obs_m_swarm is None else round(obs_m_swarm, 3),
                        "obs_clear_delta_m": None
                        if obs_m_leader is None or obs_m_swarm is None
                        else round(obs_m_leader - obs_m_swarm, 3),
                        "compress_trigger": compress_trigger,
                        "safe_dist": safe_dist,
                        "formation": formation,
                    },
                    hypothesis_id="H2,H1,H6",
                )

            previous_formation = formation

            # Terminate when actually close to end_point.
            if dist_remaining < 0.35:
                final_reached = True
                break

            time.sleep(dt_sleep)

        # ---------------- Phase 4: Hover (stream current position). ----------------
        # PositionMotion has no hover() API, so we send a stationary pose
        # reference at each drone's last known position to freeze them in
        # place.
        if not self.dry_run:
            try:
                for d in self.drones:
                    try:
                        p = d.position
                    except Exception:
                        p = None
                    if p is not None:
                        self._stream_position(d, (float(p[0]), float(p[1]), z), nominal_yaw, [0.1, 0.1, 0.1])
                    elif last_leader_target is not None:
                        self._stream_position(d, last_leader_target, nominal_yaw, [0.1, 0.1, 0.1])
            except Exception as exc:
                _dbg(
                    "mission_centralised.py:run_stage4",
                    "hover failed",
                    {"error": repr(exc)},
                    hypothesis_id="H6",
                )

        _dbg(
            "mission_centralised.py:run_stage4",
            "stage4 done",
            {"final_reached": final_reached, "ticks": tick_count, "elapsed_s": round(time.time() - t_start, 2)},
            hypothesis_id="H6",
        )
        metrics.planned_waypoints = tick_count
        metrics.end_time = time.time()
        self.metrics[stage_name] = metrics
        return metrics

    def _stage_entry_xy(self, name: str) -> Optional[Tuple[float, float]]:
        """Return the (x, y) at which ``name`` expects to start in stage1-3 (run_plan stages).

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
        """Fly the whole swarm at high altitude from from_xy to to_xy.

        4 stages live in 4 different quadrants of a 10x10 stage_size grid and
        stage3 / stage4 contain 5 m tall obstacles. Cruising at 1.2 m between
        stages would slam the leader into stage3's forest on stage3->stage4,
        and into stage2's walls on stage1->stage2 in some scenarios. So
        between stages we ascend to ferry_height (default 6 m, above the
        5 m obstacle_height), translate horizontally to the next stage's
        entry point, and descend back to cruise_height. Followers stay in a
        ``line`` formation along the ferry direction so the column doesn't
        sweep sideways through anything.
        """
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
            if name == "stage4":
                _dbg(
                    "mission_centralised.py:run_selected_stages",
                    "invoking run_stage4",
                    {},
                    hypothesis_id="H5",
                )
            outputs[name] = fn()
            exit_xy = self._stage_exit_xy(name)
            if exit_xy is not None:
                last_exit = exit_xy
        return outputs

    def export_metrics(self, output_dir: Path) -> None:
        """Write ONE Chinese Markdown file per stage that actually ran.

        File layout (under ``output_dir``):
            阶段{N}_仿真结果.md   for each stage N that has metrics.

        The Markdown contains three sections:
            1. 本次仿真配置   (scenario parameters + motion parameters)
            2. 题目要求的五大性能指标 (the 5 official evaluation axes with
                                        Chinese names, numbers, pass/fail)
            3. 阵型跟踪附加指标（仅供内部分析）
            4. 结论
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        for stage_name in ("stage1", "stage2", "stage3", "stage4"):
            metrics = self.metrics.get(stage_name)
            if metrics is None:
                continue
            md_text = self._format_stage_markdown(stage_name, metrics)
            stage_num = stage_name.replace("stage", "")
            out_path = output_dir / f"阶段{stage_num}_仿真结果.md"
            out_path.write_text(md_text, encoding="utf-8")
            self._log(f"[export] wrote {out_path}")

    def _format_stage_markdown(self, stage_name: str, m: StageMetrics) -> str:
        """Emit a data-only Markdown file for one stage.

        Six sections, tables only, no prose. Rules for how to compare
        against the decentralised run live once in
        ``results/centralised/README_对比规范.md``.
        """

        def _f(v, nd=2):
            return "—" if v is None else f"{v:.{nd}f}"

        def _i(v):
            return "—" if v is None else f"{v}"

        def _b(b):
            return "—" if b is None else ("true" if b else "false")

        stage_num = stage_name.replace("stage", "")
        scenario_path = getattr(self, "scenario_path", "") or "—"
        n_drones = len(self.drones) if getattr(self, "drones", None) else 5
        sc = self.scenario
        total_ticks = max(1, m.completed_waypoints)

        L: List[str] = []
        L.append(f"# 阶段{stage_num}（集中式）")
        L.append("")
        L.append("> 对比规则见 `../README_对比规范.md`。本文件只记录数据。")
        L.append(">")
        L.append(
            "> **题目要求（README）：success rate / reconfigurability / time taken**"
            " → 见 §1 / §2 / §3。"
        )
        L.append("> §4–§6 为补充分析（非题目硬性要求），供报告里做集中式 vs 去中心化对比参考。")
        L.append("")

        # ---- Config table: every parameter that must be ALIGNED across
        # centralised vs decentralised runs is recorded here.
        L.append("## 配置（去中心化方法必须使用相同参数才能对比）")
        L.append("")
        L.append("| 参数 | 值 |")
        L.append("|---|---|")
        L.append(f"| scenario | `{scenario_path}` |")
        L.append(f"| 无人机数 | {n_drones} |")
        L.append("| 无人机物理半径（机身+桨叶） | 0.12 m |")
        L.append("| 控制方式 | 10 Hz 连续位置流（PositionMotion streaming） |")
        L.append("| 避碰阈值（严格 / 物理） | 0.30 m / 0.15 m |")
        if stage_name == "stage1" and sc.stage1 is not None:
            R = sc.stage1.diameter / 2.0
            L.append(f"| 场地中心 | {sc.stage1.stage_center} |")
            L.append(f"| 圆轨直径 | {sc.stage1.diameter} m |")
            L.append(f"| 圆轨半径 R | {R:.2f} m |")
            L.append(f"| Leader 角速度 ω | 0.20 rad/s |")
            L.append(f"| Leader 线速度 ωR | {0.20 * R:.2f} m/s |")
            L.append(f"| 巡航高度 | 1.2 m |")
            L.append(f"| 计划巡航圈数 | 3 |")
            L.append(f"| 阵型切换周期 | π/ω = {math.pi / 0.20:.2f} s（每半圈换一次） |")
            L.append(f"| 阵型过渡时长 | 4.0 s |")
            L.append(f"| 集结时长 | 3.0 s |")
        elif stage_name == "stage2" and sc.stage2 is not None:
            L.append(f"| 场地中心 | {sc.stage2.stage_center} |")
            L.append(f"| 窗口数 | {len(sc.stage2.windows)} |")
            L.append(f"| Leader 巡航线速度 | 0.30 m/s |")
            L.append(f"| 摆渡线速度 | 0.40 m/s |")
            L.append(f"| 巡航高度 | 1.2 m（穿窗段按窗口实际高度调整） |")
            L.append(f"| 阵型过渡时长 | 1.5 s |")
            L.append(f"| 集结时长 | 2.5 s |")
        elif stage_name == "stage3" and sc.stage3 is not None:
            L.append(f"| 起点 → 终点 | {sc.stage3.start_point_global} → {sc.stage3.end_point_global} |")
            L.append(f"| 树木数量 | {len(sc.stage3.obstacles_global)} |")
            L.append(f"| 树木直径 | {sc.stage3.obstacle_diameter} m |")
            L.append(f"| 树木安全半径 | 障碍半径 + 0.20 m |")
            L.append(f"| Leader 巡航线速度 | 0.35 m/s |")
            L.append(f"| 巡航高度 | 1.2 m |")
        elif stage_name == "stage4" and sc.stage4 is not None:
            L.append(f"| 起点 → 终点 | {sc.stage4.start_point_global} → {sc.stage4.end_point_global} |")
            L.append(f"| 动态障碍数量 | {sc.stage4.num_obstacles} |")
            L.append(f"| 动态障碍速度 | {sc.stage4.obstacle_velocity} m/s |")
            L.append(f"| 动态障碍直径 | {sc.stage4.obstacle_diameter} m |")
            L.append(f"| Leader 巡航线速度 | 0.80 m/s |")
            L.append(f"| 巡航高度 | 1.8 m |")
            L.append(f"| 反应式避障安全距离 | (障碍半径+0.3) + 2.0 m |")
            L.append(f"| pure-pursuit 前瞻距离 | 2.5 m |")
        L.append("")

        L.append("## 1. Success rate")
        L.append("")
        L.append("| 阈值 | 判定 |")
        L.append("|---|---|")
        L.append(f"| strict 0.30 m | {_b(m.success_strict)} |")
        L.append(f"| physical 0.15 m | {_b(m.success_physical)} |")
        L.append("")

        L.append("## 2. Reconfigurability")
        L.append("")
        L.append("| 指标 | 值 |")
        L.append("|---|---|")
        L.append(
            f"| visited/planned | {m.formations_visited}/{m.formations_planned}"
            f" = {_f(m.reconfigurability, 3)} |"
        )
        L.append(f"| formation_switches | {_i(m.formation_switches)} |")
        L.append("")

        L.append("## 3. Time taken")
        L.append("")
        L.append("### 3.1 Time taken for completion")
        L.append("")
        L.append("| 项目 | 秒 |")
        L.append("|---|---|")
        if stage_name == "stage1":
            L.append(f"| 1 圈 (2π/ω) | {_f(m.time_per_lap_s, 2)} |")
        L.append(f"| 巡航 | {_f(m.cruise_time_s, 2)} |")
        L.append(f"| 总耗时 | {_f(m.duration_sec, 2)} |")
        L.append("")
        L.append("### 3.2 Time taken for different formation types")
        L.append("")
        if m.time_per_formation:
            L.append("| 阵型 | 秒 |")
            L.append("|---|---|")
            for name, dur in m.time_per_formation.items():
                L.append(f"| {name} | {dur:.2f} |")
        else:
            L.append("—")
        L.append("")

        L.append("## 4. 补充分析：归一化指标（非官方评分）")
        L.append("")
        L.append("| 指标 | 值 |")
        L.append("|---|---|")
        L.append(f"| path_efficiency | {_f(m.path_efficiency, 3)} |")
        L.append(f"| completion_ratio | {_f(m.completion_ratio, 3)} |")
        L.append(f"| formation_rmse (m) | {_f(m.formation_rmse_mean, 3)} |")
        L.append(f"| leader_path (m) | {_f(m.leader_path_length_m, 2)} |")
        L.append(f"| ideal_path (m) | {_f(m.ideal_path_length_m, 2)} |")
        L.append("")

        L.append("## 5. 补充分析：避碰（§1 Success rate 的判定依据）")
        L.append("")
        L.append("| 阈值 | tick | % |")
        L.append("|---|---|---|")
        L.append(
            f"| 0.30 m | {m.num_pair_collision_ticks} | "
            f"{100.0 * m.num_pair_collision_ticks / total_ticks:.2f} |"
        )
        L.append(
            f"| 0.15 m | {m.num_pair_ticks_physical} | "
            f"{100.0 * m.num_pair_ticks_physical / total_ticks:.2f} |"
        )
        L.append("")
        L.append(f"- min_pair_distance: **{_f(m.min_pair_distance_m, 3)} m**")
        if m.num_obstacle_collision_ticks is not None:
            L.append(f"- obstacle_collision_ticks: {m.num_obstacle_collision_ticks}")
        L.append("")

        L.append("## 6. 补充分析：阶段专属（非官方评分）")
        L.append("")
        L.append("| 指标 | 值 |")
        L.append("|---|---|")
        if stage_name == "stage1":
            L.append(f"| 计划圈数 | 3.00 |")
            L.append(f"| 实际完成圈数 | {_f(m.laps_completed, 3)} |")
        elif stage_name == "stage2":
            L.append(
                f"| windows_passed / planned | {_i(m.windows_passed)}/{_i(m.windows_planned)} |"
            )
        elif stage_name == "stage3":
            L.append(f"| detour_ratio | {_f(m.detour_ratio, 3)} |")
        elif stage_name == "stage4":
            L.append(
                f"| min_dynamic_obstacle_distance (m) | "
                f"{_f(m.min_dynamic_obstacle_distance_m, 3)} |"
            )
            L.append(
                f"| dynamic_obstacle_encounters | {_i(m.dynamic_obstacle_encounters)} |"
            )
        L.append("")

        return "\n".join(L)


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
        help="Output directory for csv/plots",
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
    _dbg(
        "mission_centralised.py:main",
        "post rclpy.init env",
        {
            "stage": args.stage,
            "scenario": args.scenario,
            "FASTDDS_BUILTIN_TRANSPORTS": _os.environ.get("FASTDDS_BUILTIN_TRANSPORTS"),
            "RMW_IMPLEMENTATION": _os.environ.get("RMW_IMPLEMENTATION"),
            "pid": _os.getpid(),
        },
        hypothesis_id="H1",
    )
    mission = CentralisedMission(
        scenario=scenario,
        namespaces=args.namespaces,
        verbose=args.verbose,
        use_sim_time=args.use_sim_time,
        dry_run=False,
        behavior_timeout_s=args.behavior_timeout,
    )
    mission.scenario_path = args.scenario
    # Stage4 (dynamic obstacles) uses the streaming-only fast path that
    # does NOT touch TakeoffBehavior/GoToBehavior/LandBehavior. This avoids
    # the multi-minute cold-discovery and per-goal-acceptance latencies of
    # aerostack2's action-based behaviours. Stage1/2/3 keep the legacy
    # behaviour-based takeoff/land.
    use_streaming_flight = (args.stage == "stage4")

    try:
        _dbg(
            "mission_centralised.py:main",
            "before arm_and_offboard",
            {"use_streaming_flight": use_streaming_flight, "stage": args.stage},
            hypothesis_id="H2",
        )
        t0 = time.time()
        if not mission.arm_and_offboard():
            _dbg(
                "mission_centralised.py:main",
                "arm_and_offboard returned False — abort",
                {"elapsed_s": round(time.time() - t0, 2)},
                hypothesis_id="H2",
            )
            print("Arm/offboard failed.")
            return 1
        _dbg("mission_centralised.py:main", "arm_and_offboard done",
             {"elapsed_s": round(time.time() - t0, 2)}, hypothesis_id="H9")

        t0 = time.time()
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
                    _dbg(
                        "mission_centralised.py:main",
                        "streaming_takeoff failed and no drone up — abort",
                        {"zs": [round(float(d.position[2]), 3) for d in mission.drones]},
                        hypothesis_id="H3",
                    )
                    return 1
            _dbg("mission_centralised.py:main", "streaming_takeoff done",
                 {"elapsed_s": round(time.time() - t0, 2),
                  "z": [round(float(d.position[2]), 3) for d in mission.drones]},
                 hypothesis_id="H3")
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
        except Exception as exc:
            _dbg("mission_centralised.py:main",
                 "final platform reset raised",
                 {"error": repr(exc)}, hypothesis_id="H9")
        mission.shutdown()
        rclpy.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

