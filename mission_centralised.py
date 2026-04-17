#!/usr/bin/env python3
"""Centralised (leader-follower) mission for COMP0240 CW2."""

from __future__ import annotations

import argparse
import csv
import json as _json
import math
import os as _os
import time
from dataclasses import dataclass
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
    stage: str
    completed_waypoints: int = 0
    planned_waypoints: int = 0
    collisions: int = 0
    start_time: float = 0.0
    end_time: float = 0.0

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.end_time - self.start_time)

    @property
    def success_rate(self) -> float:
        if self.planned_waypoints <= 0:
            return 0.0
        return min(1.0, self.completed_waypoints / float(self.planned_waypoints))


class MissionDrone(_DroneInterfaceBase):
    """Minimal drone wrapper. Behaviours loaded lazily, NOT in __init__.

    By subclassing ``DroneInterfaceBase`` (not ``DroneInterface``) we avoid
    the 4x ``BehaviorHandler.__init__`` calls that ``DroneInterface`` does
    eagerly, each of which worst-case waits 240 s for action+service
    servers. For stage4 (streaming-only) no behaviour is ever loaded. For
    stage1/2/3 (still behaviour-based), ``_ensure_behavior`` lazily loads
    takeoff/go_to/land on first use.
    """

    # Module alias -> import path used by DroneInterfaceBase.load_module.
    _BEHAVIOUR_MODULES = {
        "takeoff": "as2_python_api.modules.takeoff_module",
        "land": "as2_python_api.modules.land_module",
        "go_to": "as2_python_api.modules.go_to_module",
        "follow_path": "as2_python_api.modules.follow_path_module",
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
            hypothesis_id="H2,H4,H9",
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

        # 1. Parallel priming of PositionMotion on each drone.
        spawn_xy: List[Tuple[float, float, float]] = []
        for d in self.drones:
            try:
                px, py, pz = (float(d.position[0]), float(d.position[1]), float(d.position[2]))
            except Exception:
                px, py, pz = 0.0, 0.0, 0.0
            spawn_xy.append((px, py, pz))

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
            hypothesis_id="H9",
        )
        if not all(ok for _, ok in primed):
            _dbg("mission_centralised.py:streaming_takeoff",
                 "priming failed for some drones; aborting", {}, hypothesis_id="H9")
            return False

        # 1b. Wait until every drone reports z>deck (or timeout). Runtime
        # evidence (term 15): drone0/drone1 logged Set control mode success
        # ~3–6 s AFTER drone3/4 — ramp started immediately and those two
        # never reached target_z in the final check ("only three took off").
        t_deck = time.time()
        deck_z = 0.22
        while time.time() - t_deck < 12.0:
            try:
                if all(float(d.position[2]) > deck_z for d in self.drones):
                    break
            except Exception:
                pass
            time.sleep(0.15)
        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "post-prime deck wait done",
            {"wait_s": round(time.time() - t_deck, 2),
             "zs": [round(float(d.position[2]), 3) for d in self.drones]},
            hypothesis_id="H9",
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
                z_real = 0.0
            final_zs.append(z_real)
            if abs(z_real - target_z) > 0.4:
                all_ok = False
        _dbg(
            "mission_centralised.py:streaming_takeoff",
            "final altitude check",
            {"target_z": target_z, "final_zs": [round(z, 3) for z in final_zs], "all_ok": all_ok},
            hypothesis_id="H9",
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
        return self.run_plan(
            "stage1",
            self.planner.plan_stage1(),
            speed=0.6,
            transition_points=3,
        )

    def run_stage2(self) -> StageMetrics:
        return self.run_plan("stage2", self.planner.plan_stage2(), speed=0.30, transition_points=3)

    def run_stage3(self) -> StageMetrics:
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
        # Absolute clamp on filtered shift -- never deflect the target more
        # than MAX_SHIFT m off the pure-pursuit base. Keeps avoidance visible
        # but bounded, and prevents the swarm from getting yanked out of the
        # stage corridor (stage4 floor: x,y in [-5,5]).
        MAX_SHIFT = 1.5
        # Slower blend: goal-corridor avoidance is stable; heavy EMA was still
        # letting residual jitter read as "乱飞".
        SHIFT_ALPHA = 0.22
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
                    lookahead_s=0.6,
                    goal=(end_x, end_y),
                )
                ddx = rx - base_x
                ddy = ry - base_y
                m = math.hypot(ddx, ddy)
                if m > raw_mag_best:
                    raw_mag_best = m
                    raw_dx_merged = ddx
                    raw_dy_merged = ddy
            raw_lx = base_x + raw_dx_merged
            raw_ly = base_y + raw_dy_merged
            raw_dx = raw_lx - base_x
            raw_dy = raw_ly - base_y
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
            outputs[name] = fn()
            exit_xy = self._stage_exit_xy(name)
            if exit_xy is not None:
                last_exit = exit_xy
        return outputs

    def export_metrics(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "centralised_metrics.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["stage", "planned_waypoints", "completed_waypoints", "success_rate", "duration_sec", "collisions"])
            for stage_name in ("stage1", "stage2", "stage3", "stage4"):
                metrics = self.metrics.get(stage_name)
                if metrics is None:
                    continue
                writer.writerow(
                    [
                        stage_name,
                        metrics.planned_waypoints,
                        metrics.completed_waypoints,
                        f"{metrics.success_rate:.3f}",
                        f"{metrics.duration_sec:.3f}",
                        metrics.collisions,
                    ]
                )

        stages = [name for name in ("stage1", "stage2", "stage3", "stage4") if name in self.metrics]
        success = [self.metrics[name].success_rate for name in stages]
        duration = [self.metrics[name].duration_sec for name in stages]
        try:
            import matplotlib.pyplot as plt

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            axes[0].bar(stages, success)
            axes[0].set_ylim(0, 1.05)
            axes[0].set_title("Success rate")
            axes[1].bar(stages, duration)
            axes[1].set_title("Duration (s)")
            fig.tight_layout()
            fig.savefig(output_dir / "centralised_metrics.png", dpi=160)
            plt.close(fig)
            return
        except Exception:
            pass

        # Fallback lightweight SVG plot so metrics always include a figure artifact.
        bar_width = 70
        spacing = 40
        chart_height = 220
        max_duration = max(duration) if duration else 1.0
        total_width = max(500, (bar_width + spacing) * max(1, len(stages)) + 140)
        total_height = 420

        svg_lines: List[str] = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="{total_height}">',
            '<rect width="100%" height="100%" fill="white"/>',
            '<text x="20" y="30" font-size="20" font-family="sans-serif">Centralised mission metrics</text>',
            '<text x="20" y="55" font-size="14" font-family="sans-serif">Left chart: success rate | Right chart: duration</text>',
        ]

        left_x0 = 40
        right_x0 = (total_width // 2) + 20
        y0 = 320

        svg_lines.append(f'<line x1="{left_x0}" y1="{y0}" x2="{(total_width // 2) - 20}" y2="{y0}" stroke="black"/>')
        svg_lines.append(f'<line x1="{right_x0}" y1="{y0}" x2="{total_width - 40}" y2="{y0}" stroke="black"/>')

        for idx, stage in enumerate(stages):
            sx = left_x0 + 20 + idx * (bar_width + spacing)
            sy = success[idx]
            height = int(sy * chart_height)
            svg_lines.append(
                f'<rect x="{sx}" y="{y0 - height}" width="{bar_width}" height="{height}" fill="#4C78A8" />'
            )
            svg_lines.append(
                f'<text x="{sx}" y="{y0 + 20}" font-size="12" font-family="sans-serif">{stage}</text>'
            )

            dx = right_x0 + 20 + idx * (bar_width + spacing)
            dv = duration[idx]
            d_height = int((dv / max(max_duration, 1e-6)) * chart_height)
            svg_lines.append(
                f'<rect x="{dx}" y="{y0 - d_height}" width="{bar_width}" height="{d_height}" fill="#F58518" />'
            )
            svg_lines.append(
                f'<text x="{dx}" y="{y0 + 20}" font-size="12" font-family="sans-serif">{stage}</text>'
            )

        svg_lines.append("</svg>")
        (output_dir / "centralised_metrics.svg").write_text("\n".join(svg_lines), encoding="utf-8")


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
    # Stage4 (dynamic obstacles) uses the streaming-only fast path that
    # does NOT touch TakeoffBehavior/GoToBehavior/LandBehavior. This avoids
    # the multi-minute cold-discovery and per-goal-acceptance latencies of
    # aerostack2's action-based behaviours. Stage1/2/3 keep the legacy
    # behaviour-based takeoff/land.
    use_streaming_flight = (args.stage == "stage4")

    try:
        t0 = time.time()
        if not mission.arm_and_offboard():
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
                    return 1
            _dbg("mission_centralised.py:main", "streaming_takeoff done",
                 {"elapsed_s": round(time.time() - t0, 2),
                  "z": [round(float(d.position[2]), 3) for d in mission.drones]},
                 hypothesis_id="H9")
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

