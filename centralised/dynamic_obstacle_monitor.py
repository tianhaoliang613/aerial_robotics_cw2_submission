"""ROS2 subscriber that tracks moving obstacle positions for stage4.

In Gazebo mode the `libDynamicMovingObjects` plugin publishes each pillar's
pose on the Gazebo topic ``/dynamic_obstacles/locations`` which is bridged to
ROS2 as ``geometry_msgs/msg/Pose`` (one message per pillar, no header, no ID).
Because the message carries no identifier, we recover per-pillar tracks by
*spatial clustering*: each incoming Pose is assigned to the nearest existing
track within a gate distance; if no track is close enough a new track is
created (capped at ``max_tracks``). Velocities are estimated by a smoothed
finite difference using the ROS clock of the subscriber node.

The monitor spins its own ROS2 Node in a background daemon thread so the main
mission loop can still block on drone behaviors.
"""

from __future__ import annotations

import math
import threading
from typing import Dict, List, Optional

try:
    import rclpy
    from rclpy.executors import SingleThreadedExecutor
    from geometry_msgs.msg import Pose
except ImportError:  # pragma: no cover - dry-run without ROS2 sourced
    rclpy = None  # type: ignore[assignment]
    SingleThreadedExecutor = object  # type: ignore[assignment]
    Pose = None  # type: ignore[assignment]

from .obstacle_avoidance import DynamicObstacle


class _Track:
    __slots__ = ("x", "y", "t", "vx", "vy")

    def __init__(self, x: float, y: float, t: float) -> None:
        self.x = x
        self.y = y
        self.t = t
        self.vx = 0.0
        self.vy = 0.0


class DynamicObstacleMonitor:
    """Track dynamic obstacle positions by spatial clustering of Pose messages."""

    def __init__(
        self,
        obstacle_diameter: float = 0.5,
        topic: str = "/dynamic_obstacles/locations",
        max_tracks: int = 5,
        gate_distance: float = 1.0,
        track_ttl: float = 2.0,
    ) -> None:
        self.obstacle_diameter = obstacle_diameter
        self._topic = topic
        self._max_tracks = max_tracks
        self._gate_distance = gate_distance
        self._track_ttl = track_ttl

        self._lock = threading.Lock()
        self._tracks: List[_Track] = []
        self._msg_count: int = 0

        self._node = None  # type: ignore[assignment]
        self._executor = None
        self._thread: Optional[threading.Thread] = None
        self._started = False

    def start(self) -> None:
        if rclpy is None or self._started:
            return
        self._node = rclpy.create_node("dynamic_obstacle_monitor")
        try:
            self._node.declare_parameter("use_sim_time", True)
        except Exception:
            pass
        self._node.create_subscription(Pose, self._topic, self._cb, 50)
        self._executor = SingleThreadedExecutor()
        self._executor.add_node(self._node)
        self._thread = threading.Thread(target=self._executor.spin, daemon=True)
        self._thread.start()
        self._started = True

    def shutdown(self) -> None:
        if not self._started:
            return
        self._started = False
        try:
            self._executor.shutdown()
        except Exception:
            pass
        try:
            self._node.destroy_node()
        except Exception:
            pass

    def _now(self) -> float:
        if self._node is None:
            return 0.0
        try:
            return self._node.get_clock().now().nanoseconds * 1e-9
        except Exception:
            return 0.0

    def _cb(self, msg) -> None:
        x = float(msg.position.x)
        y = float(msg.position.y)
        t = self._now()

        with self._lock:
            self._msg_count += 1
            # Drop stale tracks so reappearing obstacles don't keep being
            # associated with an outdated estimate.
            self._tracks = [tr for tr in self._tracks if t - tr.t < self._track_ttl]

            # Nearest-neighbor assignment.
            best_idx = -1
            best_d = self._gate_distance
            for i, tr in enumerate(self._tracks):
                d = math.hypot(tr.x - x, tr.y - y)
                if d < best_d:
                    best_d = d
                    best_idx = i

            if best_idx < 0:
                if len(self._tracks) < self._max_tracks:
                    self._tracks.append(_Track(x, y, t))
                return

            tr = self._tracks[best_idx]
            dt = t - tr.t
            if dt > 1e-3:
                raw_vx = (x - tr.x) / dt
                raw_vy = (y - tr.y) / dt
                alpha = 0.5
                tr.vx = alpha * raw_vx + (1.0 - alpha) * tr.vx
                tr.vy = alpha * raw_vy + (1.0 - alpha) * tr.vy
            tr.x = x
            tr.y = y
            tr.t = t

    def snapshot(self) -> List[DynamicObstacle]:
        radius = self.obstacle_diameter / 2.0
        out: List[DynamicObstacle] = []
        t_now = self._now()
        with self._lock:
            for tr in self._tracks:
                if t_now - tr.t >= self._track_ttl:
                    continue
                out.append(DynamicObstacle(x=tr.x, y=tr.y, vx=tr.vx, vy=tr.vy, radius=radius))
        return out

    def count(self) -> int:
        with self._lock:
            return len(self._tracks)

    def stats(self) -> Dict[str, float]:
        with self._lock:
            return {
                "tracks": float(len(self._tracks)),
                "msg_count": float(self._msg_count),
            }
