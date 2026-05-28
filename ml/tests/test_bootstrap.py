"""Unit tests for the bootstrap synthetic-data generator (Phase 7a)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ml import bootstrap


def test_synthetic_record_shape_matches_feature_snapshot():
    rec = bootstrap.synthetic_record(
        route_id="720",
        window_start=datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    # Same field names + types the live feature-snapshot writes (so the same
    # Glue table can read both).
    assert isinstance(rec["route_id"], str)
    assert rec["window_start_iso"] == "2026-05-20T08:00:00Z"
    assert isinstance(rec["avg_delay_seconds"], int)
    assert isinstance(rec["p95_delay_seconds"], int)
    assert isinstance(rec["on_time_pct"], float)
    assert isinstance(rec["vehicle_count"], int)
    assert isinstance(rec["temp_c"], float)
    assert isinstance(rec["precip_mm"], float)
    assert isinstance(rec["ingested_at"], str)


def test_synthetic_record_is_deterministic_for_same_seed():
    r1 = bootstrap.synthetic_record("720", datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc), seed=42)
    r2 = bootstrap.synthetic_record("720", datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc), seed=42)
    assert r1 == r2


def test_rush_hour_delays_are_higher_than_off_peak():
    rush_records = [
        bootstrap.synthetic_record("720", datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc), seed=s)
        for s in range(100)
    ]
    offpeak_records = [
        bootstrap.synthetic_record("720", datetime(2026, 5, 20, 14, 0, 0, tzinfo=timezone.utc), seed=s)
        for s in range(100)
    ]
    rush_avg = sum(r["avg_delay_seconds"] for r in rush_records) / len(rush_records)
    off_avg = sum(r["avg_delay_seconds"] for r in offpeak_records) / len(offpeak_records)
    assert rush_avg > off_avg


def test_generate_window_iter_yields_expected_window_count():
    start = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    windows = list(bootstrap.generate_windows(start, end, window_minutes=5))
    assert len(windows) == 24 * 60 // 5  # 288 windows in a day
    assert windows[0] == start
    assert windows[-1] == end - timedelta(minutes=5)


def test_records_for_all_routes_over_range_returns_expected_total():
    start = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)  # 12 windows
    routes = ["720", "33", "2"]
    records = list(bootstrap.records_for(routes, start, end, base_seed=0))
    assert len(records) == 12 * 3
    by_route: dict[str, list] = {}
    for r in records:
        by_route.setdefault(r["route_id"], []).append(r["window_start_iso"])
    assert set(by_route.keys()) == {"720", "33", "2"}
    for route_id, isos in by_route.items():
        assert isos == sorted(isos)


def test_partition_key_for_window():
    iso = "2026-05-20T08:35:00Z"
    assert bootstrap.partition_key_for(iso) == "year=2026/month=05/day=20/hour=08"
