"""Unit tests for the feature-snapshot Lambda — pure helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

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


def _agg_rows():
    return [
        {"route_id": "720", "window_start_iso": "2026-05-27T11:50:00Z",
         "avg_delay_seconds": Decimal("87"), "p95_delay_seconds": Decimal("240"),
         "on_time_pct": "71.4", "vehicle_count": Decimal("9")},
        {"route_id": "33", "window_start_iso": "2026-05-27T11:50:00Z",
         "avg_delay_seconds": Decimal("0"), "p95_delay_seconds": Decimal("0"),
         "on_time_pct": "100.0", "vehicle_count": Decimal("2")},
    ]


def test_lambda_handler_writes_one_s3_object_with_n_lines(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "ARCHIVE_BUCKET", "bkt")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index")

    ra_table = MagicMock()
    ra_table.query.return_value = {"Items": _agg_rows()}
    wc_table = MagicMock()
    s3_client = MagicMock()

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra_table)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc_table)
    monkeypatch.setattr(handler, "_s3", lambda: s3_client)
    monkeypatch.setattr(
        handler, "fetch_weather",
        lambda: {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"},
    )
    fixed_now = datetime(2026, 5, 27, 12, 2, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(handler, "_utcnow", lambda: fixed_now)

    result = handler.lambda_handler({}, MagicMock())

    assert result["ok"] is True
    assert result["window_start_iso"] == "2026-05-27T11:50:00Z"
    assert result["records_written"] == 2

    # Exactly one S3 PUT this cycle.
    assert s3_client.put_object.call_count == 1
    kwargs = s3_client.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "bkt"
    assert kwargs["Key"].startswith(
        "processed-features/year=2026/month=05/day=27/hour=11/"
    )
    assert kwargs["Key"].endswith(".jsonl.gz")
    assert kwargs["ContentType"] == "application/x-ndjson"
    assert kwargs["ContentEncoding"] == "gzip"
    import gzip
    decoded = gzip.decompress(kwargs["Body"]).decode("utf-8").splitlines()
    assert len(decoded) == 2
    parsed = [json.loads(line) for line in decoded]
    assert {p["route_id"] for p in parsed} == {"720", "33"}
    assert all("temp_c" in p for p in parsed)

    # Weather-cache row was upserted with TTL.
    wc_table.put_item.assert_called_once()
    cache_item = wc_table.put_item.call_args.kwargs["Item"]
    assert cache_item["id"] == "la"
    # Stored as Decimal (DynamoDB rejects float); compare against Decimal since
    # Decimal("22.4") != the float 22.4 due to binary-float imprecision.
    assert cache_item["temp_c"] == Decimal("22.4")
    assert cache_item["precip_mm"] == Decimal("0.0")
    assert cache_item["ttl_epoch"] > int(fixed_now.timestamp())


def test_upsert_weather_cache_uses_decimal_not_float(monkeypatch):
    """DynamoDB's resource API rejects Python float — weather values must be
    Decimal. Regression: a live invoke raised 'Float types are not supported.
    Use Decimal types instead.' because Open-Meteo returns floats."""
    wc_table = MagicMock()
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc_table)
    now = datetime(2026, 5, 27, 12, 2, 30, tzinfo=timezone.utc)

    handler.upsert_weather_cache(
        {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"}, now
    )

    item = wc_table.put_item.call_args.kwargs["Item"]
    assert isinstance(item["temp_c"], Decimal)
    assert isinstance(item["precip_mm"], Decimal)
    assert item["temp_c"] == Decimal("22.4")
    assert item["precip_mm"] == Decimal("0.0")


def test_lambda_handler_writes_records_without_weather_when_open_meteo_fails(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "ARCHIVE_BUCKET", "bkt")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index")

    ra_table = MagicMock()
    ra_table.query.return_value = {"Items": _agg_rows()}
    wc_table = MagicMock()
    s3_client = MagicMock()

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra_table)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc_table)
    monkeypatch.setattr(handler, "_s3", lambda: s3_client)
    monkeypatch.setattr(handler, "fetch_weather", lambda: None)  # simulate failure
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 5, 27, 12, 5, 30, tzinfo=timezone.utc),
    )

    result = handler.lambda_handler({}, MagicMock())
    assert result["ok"] is True
    assert result["records_written"] == 2
    assert s3_client.put_object.call_count == 1
    # Cache was NOT upserted (no weather to cache).
    wc_table.put_item.assert_not_called()


def test_lambda_handler_no_rows_for_window_skips_s3_but_still_caches_weather(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "ARCHIVE_BUCKET", "bkt")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index")

    ra_table = MagicMock()
    ra_table.query.return_value = {"Items": []}
    wc_table = MagicMock()
    s3_client = MagicMock()

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra_table)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc_table)
    monkeypatch.setattr(handler, "_s3", lambda: s3_client)
    monkeypatch.setattr(
        handler, "fetch_weather",
        lambda: {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"},
    )
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 5, 27, 12, 5, 30, tzinfo=timezone.utc),
    )

    result = handler.lambda_handler({}, MagicMock())
    assert result["ok"] is True
    assert result["records_written"] == 0
    s3_client.put_object.assert_not_called()
    # Weather cache still updated — useful for the precompute Lambda even on a quiet cycle.
    wc_table.put_item.assert_called_once()


def test_lambda_handler_pagination_drains_all_gsi_pages(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "ARCHIVE_BUCKET", "bkt")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index")

    ra_table = MagicMock()
    page1 = _agg_rows()
    page2 = [{"route_id": "2", "window_start_iso": "2026-05-27T11:50:00Z",
              "avg_delay_seconds": Decimal("10"), "p95_delay_seconds": Decimal("30"),
              "on_time_pct": "95.0", "vehicle_count": Decimal("3")}]
    ra_table.query.side_effect = [
        {"Items": page1, "LastEvaluatedKey": {"window_start_iso": "x", "route_id": "33"}},
        {"Items": page2},
    ]
    wc_table = MagicMock()
    s3_client = MagicMock()
    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra_table)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc_table)
    monkeypatch.setattr(handler, "_s3", lambda: s3_client)
    monkeypatch.setattr(
        handler, "fetch_weather",
        lambda: {"temp_c": 22.4, "precip_mm": 0.0, "observed_at": "2026-05-27T12:00"},
    )
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 5, 27, 12, 5, 30, tzinfo=timezone.utc),
    )

    result = handler.lambda_handler({}, MagicMock())
    assert ra_table.query.call_count == 2  # both pages read
    assert result["records_written"] == 3


def test_fetch_weather_real_url_construction_and_parse(monkeypatch):
    """fetch_weather uses urllib.request; patch urlopen and assert URL + parsing."""
    fake_resp = MagicMock()
    fake_resp.read.return_value = (
        b'{"current": {"temperature_2m": 19.1, "precipitation": 0.3, '
        b'"time": "2026-05-27T12:00"}}'
    )
    fake_resp.__enter__ = lambda self: self
    fake_resp.__exit__ = lambda self, *a: None

    captured = {}

    def fake_urlopen(url, timeout):
        captured["url"] = url
        captured["timeout"] = timeout
        return fake_resp

    monkeypatch.setattr(handler.urllib.request, "urlopen", fake_urlopen)
    result = handler.fetch_weather()
    assert result == {"temp_c": 19.1, "precip_mm": 0.3, "observed_at": "2026-05-27T12:00"}
    assert "api.open-meteo.com" in captured["url"]
    assert "latitude=34.05" in captured["url"]
    assert "longitude=-118.24" in captured["url"]
    assert "temperature_2m" in captured["url"]
    assert "precipitation" in captured["url"]


def test_fetch_weather_swallows_http_failure(monkeypatch):
    def fake_urlopen(url, timeout):
        raise OSError("network down")

    monkeypatch.setattr(handler.urllib.request, "urlopen", fake_urlopen)
    assert handler.fetch_weather() is None
