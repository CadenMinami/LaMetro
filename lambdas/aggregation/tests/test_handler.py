"""Unit tests for the aggregation Lambda."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from lambdas.aggregation import handler


def test_floor_to_window_aligns_to_5_minute_buckets():
    # 12:03:42 → 12:00:00 in a 5-min window
    now = datetime(2026, 5, 7, 12, 3, 42, tzinfo=timezone.utc)
    assert handler.floor_to_window(now, 5) == datetime(
        2026, 5, 7, 12, 0, 0, tzinfo=timezone.utc
    )
    # 12:07:42 → 12:05:00
    now = datetime(2026, 5, 7, 12, 7, 42, tzinfo=timezone.utc)
    assert handler.floor_to_window(now, 5) == datetime(
        2026, 5, 7, 12, 5, 0, tzinfo=timezone.utc
    )
    # On-the-boundary stays on the boundary
    now = datetime(2026, 5, 7, 12, 5, 0, tzinfo=timezone.utc)
    assert handler.floor_to_window(now, 5) == now


def test_iso_z_format():
    when = datetime(2026, 5, 7, 12, 5, 0, tzinfo=timezone.utc)
    assert handler.iso_z(when) == "2026-05-07T12:05:00Z"


def test_percentile_basic():
    # Median of [1,2,3,4,5] = 3
    assert handler.percentile([1, 2, 3, 4, 5], 50) == 3.0
    # 95th of a sorted run-up
    assert handler.percentile(list(range(1, 101)), 95) == pytest.approx(95.05)
    # Single-element list returns itself for any percentile
    assert handler.percentile([42], 99) == 42.0


def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        handler.percentile([], 50)


def test_aggregate_by_route_skips_empty_route_id():
    # Out-of-service vehicles (route_id="") shouldn't contribute to any route.
    vehicles = [
        {"route_id": "720", "vehicle_id": "1", "delay_seconds": 30},
        {"route_id": "", "vehicle_id": "deadhead", "delay_seconds": None},
        {"route_id": "720", "vehicle_id": "2", "delay_seconds": 90},
    ]
    out = handler.aggregate_by_route(vehicles)
    assert set(out.keys()) == {"720"}
    assert out["720"]["vehicle_count"] == 2


def test_aggregate_by_route_with_real_delays():
    # Three vehicles on route 33: delays of 0, 60, 240 seconds
    vehicles = [
        {"route_id": "33", "vehicle_id": "a", "delay_seconds": 0},
        {"route_id": "33", "vehicle_id": "b", "delay_seconds": 60},
        {"route_id": "33", "vehicle_id": "c", "delay_seconds": 240},
    ]
    out = handler.aggregate_by_route(vehicles)
    agg = out["33"]
    assert agg["vehicle_count"] == 3
    assert agg["avg_delay_seconds"] == 100  # int((0+60+240)/3)
    # 2 of 3 are within ±60s tolerance → 66.7%
    assert agg["on_time_pct"] == pytest.approx(66.7, abs=0.05)
    # P95 of [0, 60, 240] interpolates between 60 and 240
    assert agg["p95_delay_seconds"] >= 60


def test_aggregate_by_route_handles_decimal_from_dynamodb():
    # boto3 resource API returns numbers as Decimal — make sure _to_int copes.
    vehicles = [
        {"route_id": "2", "vehicle_id": "x", "delay_seconds": Decimal("30")},
        {"route_id": "2", "vehicle_id": "y", "delay_seconds": Decimal("90")},
    ]
    out = handler.aggregate_by_route(vehicles)
    assert out["2"]["avg_delay_seconds"] == 60


def test_aggregate_by_route_all_null_delays_phase_4b_state():
    # In 4b, every vehicle's delay_seconds is None until 4c lights it up.
    # The aggregate should still emit the row with null delay fields.
    vehicles = [
        {"route_id": "720", "vehicle_id": "a", "delay_seconds": None},
        {"route_id": "720", "vehicle_id": "b", "delay_seconds": None},
    ]
    out = handler.aggregate_by_route(vehicles)
    agg = out["720"]
    assert agg["vehicle_count"] == 2
    assert agg["avg_delay_seconds"] is None
    assert agg["p95_delay_seconds"] is None
    assert agg["on_time_pct"] is None


def test_write_aggregates_omits_null_delay_fields():
    # DynamoDB rejects None — we should write the row but only include the
    # delay attributes when they're non-null.
    table = MagicMock()
    batch = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = batch

    aggregates = {
        "720": {
            "vehicle_count": 4,
            "avg_delay_seconds": None,
            "p95_delay_seconds": None,
            "on_time_pct": None,
        },
        "33": {
            "vehicle_count": 2,
            "avg_delay_seconds": 60,
            "p95_delay_seconds": 120,
            "on_time_pct": 50.0,
        },
    }
    window_start = datetime(2026, 5, 7, 12, 5, 0, tzinfo=timezone.utc)
    written = handler.write_aggregates(table, aggregates, window_start)
    assert written == 2
    items = [c.kwargs["Item"] for c in batch.put_item.call_args_list]
    by_route = {it["route_id"]: it for it in items}
    # 720 has no delay attrs
    assert "avg_delay_seconds" not in by_route["720"]
    assert by_route["720"]["vehicle_count"] == 4
    assert by_route["720"]["window_start_iso"] == "2026-05-07T12:05:00Z"
    # 33 carries the full set
    assert by_route["33"]["avg_delay_seconds"] == 60
    assert by_route["33"]["p95_delay_seconds"] == 120
    assert by_route["33"]["on_time_pct"] == "50.0"
    # Both share the same TTL = window_start + 7 days
    assert by_route["720"]["ttl_epoch"] == by_route["33"]["ttl_epoch"]


def test_lambda_handler_end_to_end(monkeypatch):
    """Wire the whole handler with mocked DDB and verify the log payload."""
    monkeypatch.setattr(handler, "HOT_TABLE", "fake-hot")
    monkeypatch.setattr(handler, "AGG_TABLE", "fake-agg")

    # Pretend hot-vehicles has 3 vehicles across 2 routes + 1 deadhead.
    fake_hot = MagicMock()
    fake_agg = MagicMock()
    fake_hot.scan.return_value = {
        "Items": [
            {"route_id": "720", "vehicle_id": "a", "delay_seconds": None},
            {"route_id": "720", "vehicle_id": "b", "delay_seconds": None},
            {"route_id": "33", "vehicle_id": "c", "delay_seconds": None},
            {"route_id": "", "vehicle_id": "deadhead", "delay_seconds": None},
        ],
        # No LastEvaluatedKey → single page.
    }

    def fake_get_table(name: str):
        return {"fake-hot": fake_hot, "fake-agg": fake_agg}[name]

    monkeypatch.setattr(handler, "get_table", fake_get_table)

    result = handler.lambda_handler({}, MagicMock())
    assert result["ok"] is True
    assert result["routes_written"] == 2  # 720 and 33; deadhead skipped
    assert result["vehicles_in_window"] == 4
