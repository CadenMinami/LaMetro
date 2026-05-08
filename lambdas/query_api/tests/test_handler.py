"""Unit tests for the query API Lambda."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from lambdas.query_api import handler
from lambdas.shared.gtfs_static import GTFSStatic


def _vehicles_event(qs: dict[str, str]) -> dict:
    return {"resource": "/vehicles", "httpMethod": "GET", "queryStringParameters": qs}


def _aggregates_event(route_id: str, qs: dict[str, str] | None = None) -> dict:
    return {
        "resource": "/routes/{routeId}/aggregates",
        "httpMethod": "GET",
        "pathParameters": {"routeId": route_id},
        "queryStringParameters": qs or {},
    }


def test_parse_bbox_happy_path():
    lon_min, lat_min, lon_max, lat_max = handler._parse_bbox("-118.30,34.02,-118.20,34.10")
    assert lon_min == pytest.approx(-118.30)
    assert lat_max == pytest.approx(34.10)


def test_parse_bbox_rejects_inverted():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.20,34.10,-118.30,34.02")


def test_parse_bbox_rejects_too_few_floats():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.30,34.02,-118.20")


def test_covering_geohashes_returns_at_least_one_cell():
    cells = handler.covering_geohashes(-118.26, 34.04, -118.24, 34.06)
    assert len(cells) >= 1
    assert all(c.startswith("9q") for c in cells)


def test_unknown_resource_returns_404():
    resp = handler.lambda_handler({"resource": "/something-else"}, None)
    assert resp["statusCode"] == 404


def test_vehicles_400_on_missing_bbox():
    resp = handler.lambda_handler(_vehicles_event({}), None)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "missing_bbox"


def test_vehicles_400_on_invalid_bbox():
    resp = handler.lambda_handler(_vehicles_event({"bbox": "garbage"}), None)
    assert resp["statusCode"] == 400


def test_vehicles_400_on_bbox_too_large():
    resp = handler.lambda_handler(_vehicles_event({"bbox": "-119,33,-117,35"}), None)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "bbox_too_large"


def test_vehicles_returns_within_bbox_with_int_delay():
    fake_table = MagicMock()
    # Decimal incoming from boto3 resource API; we should emit a clean int.
    fake_table.query.return_value = {
        "Items": [
            {
                "vehicle_id": "5817",
                "route_id": "720",
                "lat": "34.05",
                "lon": "-118.25",
                "last_updated": "2026-05-06T22:52:00Z",
                "delay_seconds": Decimal("325"),
            }
        ]
    }

    with patch.object(handler, "_hot", return_value=fake_table):
        resp = handler.lambda_handler(
            _vehicles_event({"bbox": "-118.26,34.04,-118.24,34.06"}), None
        )

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["count"] >= 1
    v = body["vehicles"][0]
    assert v["delay_seconds"] == 325  # int in JSON, not "325" string
    assert isinstance(v["delay_seconds"], int)


def test_vehicles_filters_by_route():
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
            {"vehicle_id": "b", "route_id": "4",   "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
        ]
    }

    with patch.object(handler, "_hot", return_value=fake_table):
        resp = handler.lambda_handler(
            _vehicles_event({"bbox": "-118.26,34.04,-118.24,34.06", "route_id": "720"}), None
        )

    body = json.loads(resp["body"])
    assert all(v["route_id"] == "720" for v in body["vehicles"])


def test_aggregates_400_on_missing_route_id():
    resp = handler.lambda_handler(
        {"resource": "/routes/{routeId}/aggregates", "pathParameters": {}}, None
    )
    assert resp["statusCode"] == 400


def test_aggregates_returns_windows_newest_first():
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {
                "route_id": "720",
                "window_start_iso": "2026-05-08T01:00:00Z",
                "vehicle_count": Decimal("18"),
                "avg_delay_seconds": Decimal("180"),
                "p95_delay_seconds": Decimal("420"),
                "on_time_pct": "33.3",
            },
            {
                "route_id": "720",
                "window_start_iso": "2026-05-08T00:55:00Z",
                "vehicle_count": Decimal("16"),
                "avg_delay_seconds": Decimal("210"),
                "p95_delay_seconds": Decimal("480"),
                "on_time_pct": "25.0",
            },
        ]
    }

    with patch.object(handler, "_agg", return_value=fake_table):
        resp = handler.lambda_handler(_aggregates_event("720"), None)

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["route_id"] == "720"
    assert body["count"] == 2
    # Confirm Decimal → int casting and on_time_pct → float
    assert body["windows"][0]["vehicle_count"] == 18
    assert isinstance(body["windows"][0]["vehicle_count"], int)
    assert body["windows"][0]["avg_delay_seconds"] == 180
    assert body["windows"][0]["on_time_pct"] == pytest.approx(33.3)


def test_aggregates_handles_phase_4b_rows_without_delays():
    """Some 4b-era rows have no delay fields; the API shouldn't blow up."""
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {
                "route_id": "720",
                "window_start_iso": "2026-05-07T22:00:00Z",
                "vehicle_count": Decimal("4"),
                # no avg/p95/on_time_pct
            }
        ]
    }
    with patch.object(handler, "_agg", return_value=fake_table):
        resp = handler.lambda_handler(_aggregates_event("720"), None)
    body = json.loads(resp["body"])
    w = body["windows"][0]
    assert w["avg_delay_seconds"] is None
    assert w["p95_delay_seconds"] is None
    assert w["on_time_pct"] is None


# ---------------------------------------------------------------------------
# Phase 4d — /stops and /stops/{stopId}/arrivals
# ---------------------------------------------------------------------------

def _stops_event() -> dict:
    return {"resource": "/stops", "httpMethod": "GET"}


def _arrivals_event(stop_id: str, qs: dict[str, str] | None = None) -> dict:
    return {
        "resource": "/stops/{stopId}/arrivals",
        "httpMethod": "GET",
        "pathParameters": {"stopId": stop_id},
        "queryStringParameters": qs or {},
    }


def _make_static(*, now: dt.datetime, with_owl: bool = False) -> GTFSStatic:
    """Build a GTFSStatic with one stop served by two trips, anchored to
    `now` so the synthetic arrival is exactly 5 min in the future."""
    arr_s = (now.hour * 3600 + now.minute * 60 + now.second) + 300  # +5 min
    rows = [("trip-WD", "204", arr_s, 14)]
    if with_owl:
        # An owl arrival 1 hour ahead — should appear when horizon allows.
        rows.append(("trip-OWL", "720", arr_s + 3600, 9))
    return GTFSStatic(
        feed_version="test",
        shapes={},
        trip_idx={},
        trip_route={"trip-WD": "204", "trip-OWL": "720"},
        stops={
            "stop-1": {"name": "Vermont/Sunset", "lat": 34.097, "lon": -118.291,
                       "code": None, "parent_station": None},
        },
        stop_arrivals={"stop-1": tuple(rows)},
        # Empty service_calendar = "every service active" (the loader's
        # fallback when calendar.txt is missing).
        service_calendar={},
        service_exceptions={},
        trip_service={"trip-WD": "WD", "trip-OWL": "WD"},
    )


def test_stops_returns_full_list():
    static = _make_static(now=dt.datetime(2026, 5, 8, 14, 0, tzinfo=handler._LA_TZ))
    with patch.object(handler, "_gtfs", return_value=static):
        resp = handler.lambda_handler(_stops_event(), None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["count"] == 1
    assert body["version"] == "test"
    s = body["stops"][0]
    assert s["id"] == "stop-1"
    assert s["routes"] == ["204"]


def test_stops_503_when_gtfs_unavailable():
    with patch.object(handler, "_gtfs", side_effect=RuntimeError("boom")):
        resp = handler.lambda_handler(_stops_event(), None)
    assert resp["statusCode"] == 503
    assert json.loads(resp["body"])["error"] == "gtfs_unavailable"


def test_arrivals_unknown_stop_returns_404():
    static = _make_static(now=dt.datetime(2026, 5, 8, 14, 0, tzinfo=handler._LA_TZ))
    with patch.object(handler, "_gtfs", return_value=static):
        resp = handler.lambda_handler(_arrivals_event("nope"), None)
    assert resp["statusCode"] == 404
    assert json.loads(resp["body"])["error"] == "stop_not_found"


def test_arrivals_invalid_horizon_returns_400():
    static = _make_static(now=dt.datetime(2026, 5, 8, 14, 0, tzinfo=handler._LA_TZ))
    with patch.object(handler, "_gtfs", return_value=static):
        resp = handler.lambda_handler(
            _arrivals_event("stop-1", {"horizon_minutes": "9999"}), None,
        )
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "invalid_horizon"


@pytest.fixture
def frozen_now():
    """Freeze datetime.now(UTC) so wall-clock math in the handler is
    deterministic. Returns the frozen moment as both UTC and LA local."""
    fixed_local = dt.datetime(2026, 5, 8, 13, 55, tzinfo=handler._LA_TZ)
    fixed_utc = fixed_local.astimezone(dt.timezone.utc)

    class _FakeDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_utc.replace(tzinfo=None)
            return fixed_utc.astimezone(tz)

    with patch.object(handler.dt, "datetime", _FakeDateTime):
        yield fixed_local


def test_arrivals_predicted_minutes_match_schedule_when_no_live_vehicle(frozen_now):
    """Pre-Phase-4c (no live match) → status='scheduled', delay null,
    predicted == scheduled."""
    static = _make_static(now=frozen_now)
    fake_hot = MagicMock()
    fake_hot.query.return_value = {"Items": []}  # no live vehicles
    with patch.object(handler, "_gtfs", return_value=static), \
         patch.object(handler, "_hot", return_value=fake_hot):
        resp = handler.lambda_handler(_arrivals_event("stop-1"), None)
    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200
    assert body["stop_id"] == "stop-1"
    assert body["stop_name"] == "Vermont/Sunset"
    assert len(body["arrivals"]) == 1
    a = body["arrivals"][0]
    assert a["status"] == "scheduled"
    assert a["delay_seconds"] is None
    assert a["vehicle_id"] is None
    assert a["scheduled_arrival"] == a["predicted_arrival"]
    assert a["predicted_minutes"] == 5  # exactly +5 min from frozen_now


def test_arrivals_match_live_vehicle_and_apply_delay(frozen_now):
    static = _make_static(now=frozen_now)
    fake_hot = MagicMock()
    fake_hot.query.return_value = {
        "Items": [
            {
                "vehicle_id": "5817",
                "trip_id": "trip-WD",
                "route_id": "204",
                "delay_seconds": Decimal("120"),  # 2 min late
                "last_updated": "2026-05-08T20:54:50Z",
            }
        ]
    }
    with patch.object(handler, "_gtfs", return_value=static), \
         patch.object(handler, "_hot", return_value=fake_hot):
        resp = handler.lambda_handler(_arrivals_event("stop-1"), None)
    body = json.loads(resp["body"])
    a = body["arrivals"][0]
    assert a["status"] == "live"
    assert a["delay_seconds"] == 120
    assert a["vehicle_id"] == "5817"
    # Predicted = scheduled (+5 min) + delay (+2 min) = 7 min from now.
    assert a["predicted_minutes"] == 7
    assert a["scheduled_arrival"] != a["predicted_arrival"]


def test_arrivals_due_status_when_within_60s(frozen_now):
    """A live vehicle whose predicted arrival is < 60s away surfaces as
    `due` rather than `live`."""
    # Build a stop arrival that's 30 s away.
    arr_s = (frozen_now.hour * 3600 + frozen_now.minute * 60 + 30)
    static = GTFSStatic(
        feed_version="test",
        shapes={},
        trip_idx={},
        trip_route={"trip-DUE": "204"},
        stops={"stop-1": {"name": "x", "lat": 0.0, "lon": 0.0,
                          "code": None, "parent_station": None}},
        stop_arrivals={"stop-1": (("trip-DUE", "204", arr_s, 1),)},
        service_calendar={},
        service_exceptions={},
        trip_service={"trip-DUE": "WD"},
    )
    fake_hot = MagicMock()
    fake_hot.query.return_value = {
        "Items": [{
            "vehicle_id": "v1", "trip_id": "trip-DUE", "route_id": "204",
            "delay_seconds": None, "last_updated": "t",
        }]
    }
    with patch.object(handler, "_gtfs", return_value=static), \
         patch.object(handler, "_hot", return_value=fake_hot):
        resp = handler.lambda_handler(_arrivals_event("stop-1"), None)
    a = json.loads(resp["body"])["arrivals"][0]
    assert a["status"] == "due"


def test_arrivals_horizon_excludes_far_future(frozen_now):
    """An arrival 65 min away should be excluded from a 60-min default
    horizon, but included from a 90-min horizon."""
    static = _make_static(now=frozen_now, with_owl=False)
    # Replace the single arrival with one 65 min away.
    arr_s = (frozen_now.hour * 3600 + frozen_now.minute * 60) + 65 * 60
    static.stop_arrivals["stop-1"] = (("trip-WD", "204", arr_s, 1),)
    fake_hot = MagicMock(); fake_hot.query.return_value = {"Items": []}
    with patch.object(handler, "_gtfs", return_value=static), \
         patch.object(handler, "_hot", return_value=fake_hot):
        # Default horizon = 60 min → empty
        empty = json.loads(
            handler.lambda_handler(_arrivals_event("stop-1"), None)["body"]
        )
        assert empty["arrivals"] == []
        # Bumped to 90 min → returned
        full = json.loads(
            handler.lambda_handler(
                _arrivals_event("stop-1", {"horizon_minutes": "90"}), None,
            )["body"]
        )
        assert len(full["arrivals"]) == 1


def test_arrivals_dedup_per_trip_keeps_freshest(frozen_now):
    """If the GSI returns multiple rows for the same trip (stale + fresh),
    only the freshest should be matched."""
    static = _make_static(now=frozen_now)
    fake_hot = MagicMock()
    fake_hot.query.return_value = {
        "Items": [
            {  # newest first per ScanIndexForward=False — this should win
                "vehicle_id": "fresh", "trip_id": "trip-WD", "route_id": "204",
                "delay_seconds": Decimal("60"),
                "last_updated": "2026-05-08T20:55:00Z",
            },
            {
                "vehicle_id": "stale", "trip_id": "trip-WD", "route_id": "204",
                "delay_seconds": Decimal("999"),
                "last_updated": "2026-05-08T20:00:00Z",
            },
        ]
    }
    with patch.object(handler, "_gtfs", return_value=static), \
         patch.object(handler, "_hot", return_value=fake_hot):
        resp = handler.lambda_handler(_arrivals_event("stop-1"), None)
    a = json.loads(resp["body"])["arrivals"][0]
    assert a["vehicle_id"] == "fresh"
    assert a["delay_seconds"] == 60
