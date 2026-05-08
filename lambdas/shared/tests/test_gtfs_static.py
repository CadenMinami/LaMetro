"""Unit tests for the GTFS static loader."""

from __future__ import annotations

import datetime as dt

from lambdas.shared.gtfs_static import build_static


def _synthetic_parsed() -> dict:
    """Build a tiny slim-format parsed dict resembling what the loader emits.

    Two trips:
      - trip-1 → shape-A (3 stops, schedule pre-computed)
      - trip-2 → shape-A (shared geometry, different schedule)
      - trip-3 → no shape_id → should be skipped
    """
    return {
        "feed_version": "test",
        "trips": {
            "trip-1": ("720", "shape-A"),
            "trip-2": ("720", "shape-A"),
            "trip-3": ("720", ""),  # missing shape_id
        },
        "schedules": {
            "trip-1": (
                (0, 0.0),
                (60, 920.0),
                (120, 1840.0),
            ),
            "trip-2": (
                (600, 0.0),
                (660, 920.0),
            ),
        },
        "shapes": {
            "shape-A": (
                (34.00, -118.30),
                (34.00, -118.29),
                (34.00, -118.28),
            ),
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


def test_build_static_carries_schedule_through():
    static = build_static(_synthetic_parsed())
    schedule = static.schedule_for_trip("trip-1")
    assert schedule == (
        (0, 0.0),
        (60, 920.0),
        (120, 1840.0),
    )


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


def test_build_static_skips_trip_with_no_schedule():
    parsed = _synthetic_parsed()
    parsed["schedules"].pop("trip-1")
    static = build_static(parsed)
    assert "trip-1" not in static.trip_idx
    assert "trip-2" in static.trip_idx


def test_build_static_legacy_dict_meta_fallback():
    """Old fat-format pickles store trip meta as dicts; we still read them
    so a Lambda with a stale cache doesn't crash mid-rollover."""
    parsed = _synthetic_parsed()
    parsed["trips"]["trip-1"] = {"route_id": "720", "shape_id": "shape-A"}
    static = build_static(parsed)
    assert "trip-1" in static.trip_idx
    assert static.trip_route["trip-1"] == "720"
