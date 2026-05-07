"""Unit tests for the GTFS static loader."""

from __future__ import annotations

from lambdas.shared.gtfs_static import build_static


def _synthetic_parsed() -> dict:
    """Build a tiny parsed-GTFS dict resembling what the loader produces.

    Two trips:
      - trip-1 → shape-A (3 stops, all in shape, distances provided)
      - trip-2 → shape-A (shared geometry, different schedule)
      - trip-3 → no shape_id → should be skipped
    """
    return {
        "feed_version": "test",
        "trips": {
            "trip-1": {
                "route_id": "720",
                "service_id": "WD",
                "shape_id": "shape-A",
                "direction_id": "0",
            },
            "trip-2": {
                "route_id": "720",
                "service_id": "WD",
                "shape_id": "shape-A",
                "direction_id": "1",
            },
            "trip-3": {
                "route_id": "720",
                "service_id": "WD",
                "shape_id": "",
                "direction_id": "0",
            },
        },
        "stop_times": {
            "trip-1": [
                {"stop_id": "A", "stop_sequence": 1, "arr_s": 0, "dep_s": 0,
                 "shape_dist_traveled": 0.0},
                {"stop_id": "B", "stop_sequence": 2, "arr_s": 60, "dep_s": 60,
                 "shape_dist_traveled": 920.0},
                {"stop_id": "C", "stop_sequence": 3, "arr_s": 120, "dep_s": 120,
                 "shape_dist_traveled": 1840.0},
            ],
            "trip-2": [
                {"stop_id": "A", "stop_sequence": 1, "arr_s": 600, "dep_s": 600,
                 "shape_dist_traveled": 0.0},
                {"stop_id": "B", "stop_sequence": 2, "arr_s": 660, "dep_s": 660,
                 "shape_dist_traveled": 920.0},
            ],
            "trip-3": [
                {"stop_id": "A", "stop_sequence": 1, "arr_s": 0, "dep_s": 0,
                 "shape_dist_traveled": 0.0},
            ],
        },
        "stops": {
            "A": {"lat": 34.00, "lon": -118.30, "name": "A"},
            "B": {"lat": 34.00, "lon": -118.29, "name": "B"},
            "C": {"lat": 34.00, "lon": -118.28, "name": "C"},
        },
        "shapes": {
            "shape-A": [
                (34.00, -118.30, 0.0),
                (34.00, -118.29, 920.0),
                (34.00, -118.28, 1840.0),
            ],
        },
    }


def test_build_static_skips_trip_with_no_shape():
    static = build_static(_synthetic_parsed())
    assert "trip-1" in static.trip_idx
    assert "trip-2" in static.trip_idx
    assert "trip-3" not in static.trip_idx


def test_build_static_dedupes_shapes():
    # Two trips share shape-A — only one LineString stored.
    static = build_static(_synthetic_parsed())
    assert len(static.shapes) == 1
    assert "shape-A" in static.shapes


def test_build_static_schedule_uses_provided_distances():
    static = build_static(_synthetic_parsed())
    schedule = static.schedule_for_trip("trip-1")
    assert schedule is not None
    assert len(schedule) == 3
    assert schedule[0] == (0, 0.0)
    assert schedule[1] == (60, 920.0)
    assert schedule[2] == (120, 1840.0)


def test_build_static_falls_back_to_projection_when_dist_missing():
    parsed = _synthetic_parsed()
    # Drop shape_dist_traveled from trip-1's stop_times.
    for st in parsed["stop_times"]["trip-1"]:
        st.pop("shape_dist_traveled", None)

    static = build_static(parsed)
    schedule = static.schedule_for_trip("trip-1")
    assert schedule is not None
    # Distances should be roughly 0, ~920, ~1840 (off by a few m due to
    # projection rounding).
    assert schedule[0][1] == 0.0 or schedule[0][1] < 5
    assert 900 < schedule[1][1] < 940
    assert 1800 < schedule[2][1] < 1880


def test_build_static_carries_route_id():
    static = build_static(_synthetic_parsed())
    assert static.trip_route["trip-1"] == "720"
    assert static.trip_route["trip-2"] == "720"


def test_shape_for_trip_returns_dedup_object():
    static = build_static(_synthetic_parsed())
    s1 = static.shape_for_trip("trip-1")
    s2 = static.shape_for_trip("trip-2")
    assert s1 is not None and s2 is not None
    assert s1 is s2  # same LineString object — dedup confirmation
