"""Unit tests for the precompute-predictions Lambda — pure helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from lambdas.precompute_predictions import handler


def test_derive_route_code_is_deterministic_and_bounded():
    a = handler.derive_route_code("720")
    b = handler.derive_route_code("720")
    c = handler.derive_route_code("33")
    assert a == b
    assert a != c
    assert 0 <= a < 1000
    assert 0 <= c < 1000


def test_derive_route_code_matches_crc32_modulo():
    # MUST match 7b's Athena `abs(crc32(to_utf8(route_id))) % 1000`.
    import zlib
    assert handler.derive_route_code("720") == zlib.crc32(b"720") % 1000
    assert handler.derive_route_code("33") == zlib.crc32(b"33") % 1000


def test_pick_recent_lags_returns_3_most_recent_avg_delays():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": Decimal("90")},
        {"window_start_iso": "2026-05-27T12:00:00Z", "avg_delay_seconds": Decimal("60")},
        {"window_start_iso": "2026-05-27T11:55:00Z", "avg_delay_seconds": Decimal("45")},
        {"window_start_iso": "2026-05-27T11:50:00Z", "avg_delay_seconds": Decimal("30")},
    ]
    lags = handler.pick_recent_lags(rows)
    assert lags == [90, 60, 45]


def test_pick_recent_lags_pads_with_zeros_when_insufficient_history():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": Decimal("90")},
    ]
    assert handler.pick_recent_lags(rows) == [90, 0, 0]


def test_pick_recent_lags_skips_null_delay_rows():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": None},
        {"window_start_iso": "2026-05-27T12:00:00Z", "avg_delay_seconds": Decimal("60")},
        {"window_start_iso": "2026-05-27T11:55:00Z", "avg_delay_seconds": Decimal("45")},
        {"window_start_iso": "2026-05-27T11:50:00Z", "avg_delay_seconds": Decimal("30")},
    ]
    assert handler.pick_recent_lags(rows) == [60, 45, 30]


def test_assemble_feature_csv_column_order_matches_training():
    csv = handler.assemble_feature_csv(
        route_code=42, hour_of_day=8, day_of_week=3,
        lags=[120, 90, 60], temp_c=18.5, precip_mm=0.0,
    )
    assert csv == "42,8,3,120,90,60,18.5,0.0"


def test_assemble_feature_csv_handles_null_weather():
    csv = handler.assemble_feature_csv(
        route_code=42, hour_of_day=8, day_of_week=3,
        lags=[120, 90, 60], temp_c=None, precip_mm=None,
    )
    assert csv == "42,8,3,120,90,60,0.0,0.0"


def test_prediction_item_shape():
    item = handler.build_prediction_item(
        route_id="720", predicted=132, current=75, model_version="v=2026-06-15",
        window_start_iso="2026-05-27T12:05:00Z",
        as_of=datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc),
        ttl_seconds=900,
    )
    assert item["route_id"] == "720"
    assert item["predicted_next_window_avg_delay_seconds"] == 132
    assert item["current_avg_delay_seconds"] == 75
    assert item["model_version"] == "v=2026-06-15"
    assert item["window_start_iso"] == "2026-05-27T12:05:00Z"
    assert item["as_of"] == "2026-05-27T12:06:30Z"
    assert item["ttl_epoch"] > int(datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc).timestamp())
