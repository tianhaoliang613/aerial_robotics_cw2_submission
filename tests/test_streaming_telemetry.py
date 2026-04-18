"""Tests for centralised.streaming_telemetry."""

import math

from centralised.streaming_telemetry import (
    leader_xy_increment,
    min_drone_obstacle_clearance,
    min_pairwise_xy_distance,
    pair_violation_flags,
    PAIR_PHYSICAL_M,
    PAIR_SAFETY_M,
)


def test_min_pairwise_three_points():
    pts = [(0.0, 0.0), (1.0, 0.0), (0.0, 3.0)]
    assert math.isclose(min_pairwise_xy_distance(pts) or 0.0, 1.0)


def test_pair_violation_strict_not_physical():
    d = 0.20
    strict, phys = pair_violation_flags(d)
    assert strict is True and phys is False
    assert pair_violation_flags(0.50) == (False, False)
    assert pair_violation_flags(0.10)[1] is True


def test_leader_increment():
    assert leader_xy_increment(None, (1.0, 2.0)) == 0.0
    assert leader_xy_increment((0.0, 0.0), (3.0, 4.0)) == 5.0


def test_obstacle_clearance():
    c = min_drone_obstacle_clearance([(0.0, 0.0)], [(1.0, 0.0)], obstacle_radius_m=0.25, drone_body_radius_m=0.12)
    assert c is not None
    assert abs(c - (1.0 - 0.25 - 0.12)) < 1e-9
