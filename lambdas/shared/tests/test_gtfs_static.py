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


# ---------------------------------------------------------------------------
# Phase 4d — schema v2 (stops + arrivals + service calendar)
# ---------------------------------------------------------------------------

LA_TZ = dt.timezone(dt.timedelta(hours=-7), name="LA")


def _synthetic_v2_parsed() -> dict:
    """A minimal v2 pickle with two trips, one stop, weekday-only service.

    trip-WD runs Mon-Fri ("WD" service), arrives at stop-1 at 14:00:00.
    trip-WE runs Sat-Sun ("WE" service), arrives at stop-1 at 14:05:00.
    """
    return {
        "schema_version": 2,
        "feed_version": "test-v2",
        "trips": {
            "trip-WD": ("204", "shape-A"),
            "trip-WE": ("204", "shape-A"),
        },
        "schedules": {
            "trip-WD": ((50_400, 0.0),),
            "trip-WE": ((50_700, 0.0),),
        },
        "shapes": {
            "shape-A": ((34.00, -118.30), (34.00, -118.28)),
        },
        "stops": {
            "stop-1": {"name": "Vermont/Sunset", "lat": 34.097, "lon": -118.291,
                       "code": None, "parent_station": None},
        },
        "stop_arrivals": {
            "stop-1": (
                ("trip-WD", "204", 50_400, 14),  # 14:00:00
                ("trip-WE", "204", 50_700, 14),  # 14:05:00
            ),
        },
        "service_calendar": {
            "WD": {"monday": True, "tuesday": True, "wednesday": True,
                   "thursday": True, "friday": True,
                   "saturday": False, "sunday": False,
                   "start_date": "20260101", "end_date": "20261231"},
            "WE": {"monday": False, "tuesday": False, "wednesday": False,
                   "thursday": False, "friday": False,
                   "saturday": True, "sunday": True,
                   "start_date": "20260101", "end_date": "20261231"},
        },
        "service_exceptions": {
            # 2026-12-25 falls on a Friday — remove WD service that day.
            ("20261225", "WD"): 2,
            # 2026-07-04 (Saturday) — explicitly add WD service for the
            # holiday-special schedule.
            ("20260704", "WD"): 1,
        },
        "trip_service": {
            "trip-WD": "WD",
            "trip-WE": "WE",
        },
    }


def test_v2_fields_loaded():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    assert "stop-1" in static.stops
    assert static.stops["stop-1"]["name"] == "Vermont/Sunset"
    assert "stop-1" in static.stop_arrivals
    assert static.trip_service["trip-WD"] == "WD"
    assert static.service_calendar["WD"]["monday"] is True
    assert static.service_exceptions[("20261225", "WD")] == 2


def test_v1_pickle_loads_with_empty_v2_fields():
    """A v1 pickle without the new keys must still load — empty stops etc."""
    parsed = _synthetic_parsed()  # the v1 fixture
    static = build_static(parsed)
    assert static.stops == {}
    assert static.stop_arrivals == {}
    assert static.service_calendar == {}


def test_shapes_false_skips_shape_build():
    """shapes=False should skip the shapely import entirely. Shape dict
    stays empty, but trip_route is still populated for the enrichment
    fallback path."""
    parsed = _synthetic_v2_parsed()
    static = build_static(parsed, shapes=False)
    assert static.shapes == {}
    assert static.trip_idx == {}
    assert static.trip_route["trip-WD"] == "204"


def test_is_service_active_weekday():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    # 2026-05-08 is a Friday
    friday = dt.date(2026, 5, 8)
    assert static.is_service_active("WD", friday) is True
    assert static.is_service_active("WE", friday) is False


def test_is_service_active_weekend():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    saturday = dt.date(2026, 5, 9)
    assert static.is_service_active("WD", saturday) is False
    assert static.is_service_active("WE", saturday) is True


def test_is_service_active_exception_removes():
    """Christmas 2026 is a Friday but service_exceptions strips WD that day."""
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    christmas = dt.date(2026, 12, 25)
    assert christmas.weekday() == 4  # Friday
    assert static.is_service_active("WD", christmas) is False


def test_is_service_active_exception_adds():
    """July 4 2026 is a Saturday; exception adds WD service that day."""
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    july4 = dt.date(2026, 7, 4)
    assert july4.weekday() == 5  # Saturday
    assert static.is_service_active("WD", july4) is True


def test_is_service_active_outside_window():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    too_early = dt.date(2025, 12, 31)
    assert static.is_service_active("WD", too_early) is False


def test_is_service_active_no_calendar_assumes_active():
    """When service_calendar is empty (older pickle), every service counts as
    active — better than returning zero arrivals."""
    parsed = _synthetic_v2_parsed()
    parsed["service_calendar"] = {}
    parsed["service_exceptions"] = {}
    static = build_static(parsed, shapes=False)
    assert static.is_service_active("WD", dt.date(2026, 5, 8)) is True


def test_arrivals_for_stop_filters_to_active_service():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    # Friday 13:50 local — within 60-minute horizon of trip-WD (14:00)
    # but trip-WE (Sat/Sun only) should be filtered out.
    now = dt.datetime(2026, 5, 8, 13, 50, tzinfo=LA_TZ)
    arrivals = static.arrivals_for_stop("stop-1", now, horizon_seconds=3600)
    trip_ids = [a.trip_id for a in arrivals]
    assert trip_ids == ["trip-WD"]
    assert arrivals[0].service_date == "20260508"


def test_arrivals_for_stop_horizon_boundary():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    # Exactly 5 min before the 14:00 arrival — within a 5-min horizon.
    now = dt.datetime(2026, 5, 8, 13, 55, tzinfo=LA_TZ)
    short = static.arrivals_for_stop("stop-1", now, horizon_seconds=300)
    long_ = static.arrivals_for_stop("stop-1", now, horizon_seconds=600)
    assert [a.trip_id for a in short] == ["trip-WD"]
    assert [a.trip_id for a in long_] == ["trip-WD"]
    # 4 min before, 1-min horizon → out of window
    now2 = dt.datetime(2026, 5, 8, 13, 56, tzinfo=LA_TZ)
    none_ = static.arrivals_for_stop("stop-1", now2, horizon_seconds=60)
    assert none_ == []


def test_arrivals_for_stop_lookback_window():
    """A bus that arrived 20s ago should still be visible (default lookback)."""
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    now = dt.datetime(2026, 5, 8, 14, 0, 20, tzinfo=LA_TZ)
    arrivals = static.arrivals_for_stop("stop-1", now, horizon_seconds=60)
    assert [a.trip_id for a in arrivals] == ["trip-WD"]


def test_arrivals_for_stop_unknown_returns_empty():
    static = build_static(_synthetic_v2_parsed(), shapes=False)
    now = dt.datetime(2026, 5, 8, 13, 50, tzinfo=LA_TZ)
    assert static.arrivals_for_stop("does-not-exist", now, 3600) == []


def test_arrivals_for_stop_handles_owl_service_after_midnight():
    """A trip arriving at 25:30:00 service-day yesterday (= 01:30 today)
    should still surface when "now" is 01:25 today."""
    parsed = _synthetic_v2_parsed()
    # Add an owl arrival at 25:30:00 = 91800s into Friday's service day.
    parsed["stop_arrivals"]["stop-1"] = (
        ("trip-OWL", "204", 91_800, 1),
    ) + parsed["stop_arrivals"]["stop-1"]
    parsed["trips"]["trip-OWL"] = ("204", "shape-A")
    parsed["trip_service"]["trip-OWL"] = "WD"  # owl runs on weekday service
    static = build_static(parsed, shapes=False)
    # Now = Saturday 01:25 local. Friday 25:30 = Saturday 01:30 wall-clock.
    now = dt.datetime(2026, 5, 9, 1, 25, tzinfo=LA_TZ)
    arrivals = static.arrivals_for_stop("stop-1", now, horizon_seconds=600)
    trip_ids = [a.trip_id for a in arrivals]
    assert "trip-OWL" in trip_ids
    # Service date should be Friday's, not Saturday's.
    owl = next(a for a in arrivals if a.trip_id == "trip-OWL")
    assert owl.service_date == "20260508"
