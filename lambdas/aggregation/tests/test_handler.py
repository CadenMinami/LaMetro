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
        return {"fake-hot": fake_hot, "fake-agg": fake_agg}.get(name, MagicMock())

    monkeypatch.setattr(handler, "get_table", fake_get_table)

    result = handler.lambda_handler({}, MagicMock())
    assert result["ok"] is True
    assert result["routes_written"] == 2  # 720 and 33; deadhead skipped
    assert result["vehicles_in_window"] == 4


def test_geofence_breaches_pure_logic():
    # avg_delay 360s; one geofence threshold 300 (breach, cold), one 600 (no
    # breach), one 300 but recently alerted (cooldown suppresses it).
    now_epoch = 1_700_000_000
    geofences = [
        {"user_id": "u1", "geofence_id": "g1", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal("0")},
        {"user_id": "u2", "geofence_id": "g2", "threshold_seconds": Decimal("600"),
         "enabled": True, "last_alerted_epoch": Decimal("0")},
        {"user_id": "u3", "geofence_id": "g3", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal(str(now_epoch - 60))},
        {"user_id": "u4", "geofence_id": "g4", "threshold_seconds": Decimal("300"),
         "enabled": False, "last_alerted_epoch": Decimal("0")},
    ]
    breaches = handler.geofence_breaches(geofences, avg_delay=360, now_epoch=now_epoch, cooldown=900)
    fired = {g["geofence_id"] for g in breaches}
    assert fired == {"g1"}  # g2 below threshold, g3 in cooldown, g4 disabled


def test_geofence_breaches_cooldown_elapsed():
    now_epoch = 1_700_000_000
    geofences = [
        {"user_id": "u1", "geofence_id": "g1", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal(str(now_epoch - 1000))},
    ]
    # 1000s since last alert > 900s cooldown → fires again.
    assert len(handler.geofence_breaches(geofences, 360, now_epoch, 900)) == 1


def test_build_notification_item():
    item = handler.build_notification_item(
        user_id="u1", route_id="720", avg_delay=372, threshold=300,
        now=datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc), geofence_id="g1",
    )
    assert item["user_id"] == "u1"
    assert item["route_id"] == "720"
    assert item["delay_seconds"] == 372
    assert item["threshold_seconds"] == 300
    assert item["read"] is False
    assert item["created_at"].startswith("2026-05-26T12:00:00")
    assert item["created_at"].endswith("#g1")  # unique sort key suffix
    assert "720" in item["message"]
    created_epoch = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert item["ttl_epoch"] > int(created_epoch)


def test_evaluate_geofences_paginates_and_writes_unique_notifications():
    # One route, geofences split across two GSI pages, both breaching. The
    # pagination loop must read both, and the two notifications must get
    # distinct sort keys even though `now` is identical.
    now = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    gtable = MagicMock()
    gtable.query.side_effect = [
        {"Items": [
            {"user_id": "u1", "geofence_id": "g1", "route_id": "720",
             "threshold_seconds": Decimal("300"), "enabled": True,
             "last_alerted_epoch": Decimal("0")},
        ], "LastEvaluatedKey": {"route_id": "720", "user_id": "u1"}},
        {"Items": [
            {"user_id": "u2", "geofence_id": "g2", "route_id": "720",
             "threshold_seconds": Decimal("300"), "enabled": True,
             "last_alerted_epoch": Decimal("0")},
        ]},
    ]
    ntable = MagicMock()
    aggregates = {"720": {"avg_delay_seconds": 360, "vehicle_count": 3}}

    fired = handler.evaluate_geofences(gtable, ntable, aggregates, now)

    assert fired == 2
    assert gtable.query.call_count == 2           # both pages read
    assert ntable.put_item.call_count == 2
    assert gtable.update_item.call_count == 2
    sks = {c.kwargs["Item"]["created_at"] for c in ntable.put_item.call_args_list}
    assert len(sks) == 2                           # unique sort keys


def test_evaluate_geofences_skips_routes_without_delay():
    now = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    gtable = MagicMock()
    ntable = MagicMock()
    # avg_delay None (the 4b-era / no-data case) — must not even query.
    fired = handler.evaluate_geofences(gtable, ntable, {"33": {"avg_delay_seconds": None}}, now)
    assert fired == 0
    gtable.query.assert_not_called()
    ntable.put_item.assert_not_called()


def test_evaluate_geofences_skips_routes_below_min_delay():
    # A route delayed at/below the smallest possible threshold (60s) can't trip
    # any geofence — don't waste a GSI query on it.
    now = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    gtable = MagicMock()
    ntable = MagicMock()
    fired = handler.evaluate_geofences(gtable, ntable, {"33": {"avg_delay_seconds": 60}}, now)
    assert fired == 0
    gtable.query.assert_not_called()
