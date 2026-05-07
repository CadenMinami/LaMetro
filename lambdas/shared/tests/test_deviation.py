"""Unit tests for the schedule deviation algorithm.

We use a synthetic east-west straight-line route in LA so the geometry is
trivial to reason about: 1 degree of longitude at 34°N is ~92 km, so a
0.01° step = 920 m. The 'route' has three stops every 0.01° apart, with
the bus scheduled to take 60 s between stops.
"""

from __future__ import annotations

import pytest

from lambdas.shared.deviation import (
    OFF_ROUTE_THRESHOLD_M,
    compute_delay_seconds,
    interpolate_time_at_distance,
    latlon_to_xy,
    linestring_from_latlon,
)


# Stop A at (34.00, -118.30), Stop B at (34.00, -118.29), Stop C at (34.00, -118.28).
# Heading east. ~920 m between stops at this latitude.
STOP_A = (34.00, -118.30)
STOP_B = (34.00, -118.29)
STOP_C = (34.00, -118.28)
ROUTE_SHAPE = linestring_from_latlon([STOP_A, STOP_B, STOP_C])

# Pre-compute the actual segment length so the schedule lines up perfectly.
SEG_LEN_M = ROUTE_SHAPE.project(
    type(ROUTE_SHAPE.coords[0])(*latlon_to_xy(*STOP_B))  # noqa: not actually used
) if False else None  # placeholder; we'll compute live below


def _project_dist(lat: float, lon: float) -> float:
    """Distance along ROUTE_SHAPE for a given (lat, lon)."""
    from shapely.geometry import Point
    return ROUTE_SHAPE.project(Point(*latlon_to_xy(lat, lon)))


# Schedule: leave A at t=0, arrive B at t=60, arrive C at t=120.
SCHEDULE = (
    (0, _project_dist(*STOP_A)),
    (60, _project_dist(*STOP_B)),
    (120, _project_dist(*STOP_C)),
)


def test_latlon_to_xy_preserves_relative_distances():
    # Two points 0.01° apart in longitude at LA latitude → ~922 m.
    x1, y1 = latlon_to_xy(*STOP_A)
    x2, y2 = latlon_to_xy(*STOP_B)
    dist = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
    assert 900 < dist < 950


def test_interpolate_time_basic():
    schedule = ((0, 0.0), (60, 1000.0))
    assert interpolate_time_at_distance(schedule, 0.0) == 0
    assert interpolate_time_at_distance(schedule, 500.0) == 30
    assert interpolate_time_at_distance(schedule, 1000.0) == 60


def test_interpolate_time_out_of_range_returns_none():
    schedule = ((0, 0.0), (60, 1000.0))
    assert interpolate_time_at_distance(schedule, -1.0) is None
    assert interpolate_time_at_distance(schedule, 1001.0) is None


def test_interpolate_time_empty_schedule():
    assert interpolate_time_at_distance((), 100.0) is None


def test_compute_delay_on_time():
    # Bus is at stop B exactly when scheduled (t=60).
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_B[0], STOP_B[1], 60
    )
    assert delay is not None
    assert -2 <= delay <= 2  # tiny float drift OK


def test_compute_delay_late():
    # Bus is at stop B but it's already t=180 (2 min late at B's scheduled t=60).
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_B[0], STOP_B[1], 180
    )
    assert delay is not None
    assert 118 <= delay <= 122


def test_compute_delay_early():
    # Bus is already at stop C but it's only t=60 (scheduled C arrival is 120).
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_C[0], STOP_C[1], 60
    )
    assert delay is not None
    assert -62 <= delay <= -58


def test_compute_delay_off_route_returns_none():
    # 500m north of stop B → way outside the 200m off-route threshold.
    far_north_lat = STOP_B[0] + 0.005  # ~556 m
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, far_north_lat, STOP_B[1], 60
    )
    assert delay is None


def test_compute_delay_pre_trip_returns_none():
    # 30 min before the trip starts → reject.
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_A[0], STOP_A[1], -1800
    )
    assert delay is None


def test_compute_delay_post_trip_returns_none():
    # 30 min after the trip ended.
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_C[0], STOP_C[1], 120 + 1800
    )
    assert delay is None


def test_compute_delay_within_prestart_slack():
    # 5 min before trip start, but inside the 10-min slack — should succeed.
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, STOP_A[0], STOP_A[1], -300
    )
    assert delay is not None
    assert -302 <= delay <= -298


def test_compute_delay_no_shape_returns_none():
    delay = compute_delay_seconds(None, SCHEDULE, STOP_A[0], STOP_A[1], 60)
    assert delay is None


def test_compute_delay_no_schedule_returns_none():
    delay = compute_delay_seconds(ROUTE_SHAPE, None, STOP_A[0], STOP_A[1], 60)
    assert delay is None
    delay = compute_delay_seconds(ROUTE_SHAPE, (), STOP_A[0], STOP_A[1], 60)
    assert delay is None


def test_compute_delay_just_off_route_threshold():
    # The threshold is exactly 200 m. 220 m off → reject. 180 m → accept.
    # Going north from stop B by 180 m: 180/111320 ≈ 0.00162° lat
    just_inside_lat = STOP_B[0] + 0.00161
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, just_inside_lat, STOP_B[1], 60
    )
    assert delay is not None  # 180 m, within threshold

    just_outside_lat = STOP_B[0] + 0.0020
    delay = compute_delay_seconds(
        ROUTE_SHAPE, SCHEDULE, just_outside_lat, STOP_B[1], 60
    )
    assert delay is None  # 222 m, outside threshold


def test_off_route_threshold_constant_is_200():
    # Sanity check — keep the public constant pinned.
    assert OFF_ROUTE_THRESHOLD_M == 200.0
