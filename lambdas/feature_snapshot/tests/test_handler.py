"""Unit tests for the feature-snapshot Lambda — pure helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from lambdas.feature_snapshot import handler


def test_second_to_last_closed_window_iso_at_exact_boundary():
    # 12:00:00 → the window starting 12:00 is the *current* (open) one; the
    # most recent closed is 11:55; second-to-last closed is 11:50.
    now = datetime(2026, 5, 27, 12, 0, 0, tzinfo=timezone.utc)
    assert handler.second_to_last_closed_window_iso(now) == "2026-05-27T11:50:00Z"


def test_second_to_last_closed_window_iso_mid_window():
    # 12:03:42 → current open window started 12:00; last closed 11:55; second
    # to last closed 11:50.
    now = datetime(2026, 5, 27, 12, 3, 42, tzinfo=timezone.utc)
    assert handler.second_to_last_closed_window_iso(now) == "2026-05-27T11:50:00Z"


def test_second_to_last_closed_window_iso_crosses_hour():
    # 13:01:00 → open window 13:00; last closed 12:55; second to last 12:50.
    now = datetime(2026, 5, 27, 13, 1, 0, tzinfo=timezone.utc)
    assert handler.second_to_last_closed_window_iso(now) == "2026-05-27T12:50:00Z"


def test_second_to_last_closed_window_iso_crosses_midnight():
    # 00:04 UTC → open window 00:00, last closed 23:55 (prev day),
    # second to last 23:50 (prev day).
    now = datetime(2026, 5, 27, 0, 4, 0, tzinfo=timezone.utc)
    assert handler.second_to_last_closed_window_iso(now) == "2026-05-26T23:50:00Z"


def test_parse_open_meteo_response_happy_path():
    body = (
        b'{"current": {"temperature_2m": 22.4, "precipitation": 0.0, '
        b'"time": "2026-05-27T12:00"}}'
    )
    parsed = handler.parse_open_meteo_response(body)
    assert parsed == {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"}


def test_parse_open_meteo_response_missing_current_returns_none():
    assert handler.parse_open_meteo_response(b'{"hourly": {}}') is None


def test_parse_open_meteo_response_garbage_returns_none():
    assert handler.parse_open_meteo_response(b"not json") is None


def test_build_feature_record_with_weather():
    agg_row = {
        "route_id": "720",
        "window_start_iso": "2026-05-27T11:50:00Z",
        "avg_delay_seconds": Decimal("87"),
        "p95_delay_seconds": Decimal("240"),
        "on_time_pct": "71.4",
        "vehicle_count": Decimal("9"),
    }
    weather = {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"}
    rec = handler.build_feature_record(
        agg_row, weather, ingested_at_iso="2026-05-27T12:05:30Z"
    )
    assert rec["route_id"] == "720"
    assert rec["window_start_iso"] == "2026-05-27T11:50:00Z"
    assert rec["avg_delay_seconds"] == 87  # Decimal coerced
    assert rec["p95_delay_seconds"] == 240
    assert rec["on_time_pct"] == 71.4  # str coerced to float
    assert rec["vehicle_count"] == 9
    assert rec["temp_c"] == 22.4
    assert rec["precip_mm"] == 0.0
    assert rec["ingested_at"] == "2026-05-27T12:05:30Z"
    assert rec["weather_observed_at"] == "2026-05-27T12:00"


def test_build_feature_record_without_weather_omits_those_fields():
    # When Open-Meteo failed, the record still writes — weather fields absent.
    agg_row = {
        "route_id": "33",
        "window_start_iso": "2026-05-27T11:50:00Z",
        "avg_delay_seconds": Decimal("0"),
        "p95_delay_seconds": Decimal("0"),
        "on_time_pct": "100.0",
        "vehicle_count": Decimal("2"),
    }
    rec = handler.build_feature_record(agg_row, None, "2026-05-27T12:05:30Z")
    assert "temp_c" not in rec
    assert "precip_mm" not in rec
    assert rec["route_id"] == "33"


def test_build_feature_record_handles_4b_era_null_delays():
    # Pre-4c rows can have absent delay fields entirely — handle gracefully.
    agg_row = {
        "route_id": "720",
        "window_start_iso": "2026-05-27T11:50:00Z",
        "vehicle_count": Decimal("4"),
    }
    rec = handler.build_feature_record(agg_row, None, "2026-05-27T12:05:30Z")
    assert rec["avg_delay_seconds"] is None
    assert rec["vehicle_count"] == 4
