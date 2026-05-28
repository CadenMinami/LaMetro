# Phase 7a — Data Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start durably capturing per-(route, window) records with weather to S3, with a Glue table making them Athena-queryable — the missing labeled training store that 7b/7c will build on.

**Architecture:** A new `feature-snapshot` Lambda runs every 5 min on EventBridge, reads the second-to-last closed window's per-route aggregates from a new GSI on `route-aggregates`, fetches one Open-Meteo observation for LA, writes one gzipped JSON-line per (route, window) to `s3://…/processed-features/year=…/`, and upserts the weather observation to a new `weather-cache` DDB row. A new `MLStack` houses the Lambda + schedule + Glue database/table. A standalone `ml/bootstrap.py` can seed synthetic data so 7b/7c are demoable without waiting for real data to accumulate.

**Tech Stack:** AWS CDK v2 (TypeScript), Python 3.12 Lambdas (boto3, stdlib only — `urllib.request` for the Open-Meteo HTTP call), pytest + `unittest.mock` (the repo does NOT use moto), Glue (Hive-style partition projection — no crawler needed).

**Spec:** `docs/superpowers/specs/2026-05-27-phase-7-delay-prediction-design.md`

---

## Conventions to follow (read before starting)

- **Lambda tests:** `lambdas/<name>/tests/test_handler.py`, import as `from lambdas.<name> import handler`. Use `MagicMock` + `monkeypatch`. Run from repo root with `pytest lambdas/<name> -v`. Root `pyproject.toml` sets `pythonpath=["."]` and `--import-mode=importlib`.
- **No `tests/__init__.py`** in any lambda dir (Phase 6 polish fix — having it makes every `test_handler.py` shadow each other in the combined CI run). The lambda package dir also gets no `__init__.py`. See `pyproject.toml`'s comment.
- **Lambda response shape / Decimal handling:** copy `_response`/`_json_default` from `lambdas/query_api/handler.py:176-194` if needed; coerce Decimal to int/float on output.
- **CDK verify:** from `cdk/`, `npx tsc --noEmit` then `npx cdk synth --quiet`. There are no CDK unit tests in this repo; synth + type-check is the gate.
- **Lambda build:** `scripts/build-lambda.sh <name>` produces `lambdas/<name>/.build/`. New lambdas need a `requirements.txt` (comment-only fine — boto3 + stdlib ship in the runtime) and must be added to the CI build loop in `.github/workflows/pr-checks.yml`.
- **Commits:** do NOT add `Co-Authored-By: Claude` trailer (user preference).
- **One feature-snapshot S3 write per cycle:** the Lambda writes ONE `.jsonl.gz` object containing all that window's per-route records — not one object per record. Cheaper PUT count and simpler reads.

---

## File map

| File | Status | Responsibility |
|---|---|---|
| `cdk/lib/storage-stack.ts` | modify | Add `weather-cache` table + `window_start_iso-index` GSI to `route-aggregates` |
| `cdk/lib/ml-stack.ts` | create | feature-snapshot Lambda + EventBridge 5-min schedule + Glue DB + Glue Table |
| `cdk/bin/cdk.ts` | modify | Construct & wire MLStack with storage deps |
| `lambdas/feature_snapshot/handler.py` | create | The Lambda |
| `lambdas/feature_snapshot/requirements.txt` | create | Comment-only (stdlib + runtime boto3) |
| `lambdas/feature_snapshot/tests/test_handler.py` | create | TDD tests |
| `ml/bootstrap.py` | create | Standalone synthetic-data seeder script |
| `ml/tests/test_bootstrap.py` | create | Pure-logic tests for synthetic generator |
| `.github/workflows/pr-checks.yml` | modify | Add `feature_snapshot` to the build loop |

---

## Task 1: StorageStack — add `weather-cache` table + `window_start_iso-index` GSI on `route-aggregates`

**Files:**
- Modify: `cdk/lib/storage-stack.ts`

- [ ] **Step 1: Add the new field and constructs**

In `cdk/lib/storage-stack.ts`, add a public readonly field next to the existing tables:
```typescript
  public readonly weatherCacheTable: dynamodb.Table;
```

Then, in the constructor, after the `notificationsTable` block and before the `archiveBucket` block, add:
```typescript
    // Phase 7a: tiny single-row cache holding the most recent LA weather
    // observation. Written by feature-snapshot each 5-min cycle; read by
    // precompute-predictions (7c) so a second Lambda doesn't also call
    // Open-Meteo. 10-min TTL guarantees stale rows can't linger if snapshot
    // stops running.
    this.weatherCacheTable = new dynamodb.Table(this, 'WeatherCacheTable', {
      tableName: 'la-metro-weather-cache',
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
```

Add a `window_start_iso-index` GSI to the existing `routeAggregatesTable` immediately after its construct block (before the `geofencesTable` block). Place it next to the existing GSI patterns:
```typescript
    // Phase 7a: lets the feature-snapshot Lambda fetch "all routes' aggregate
    // rows for window X" in one query instead of a full-table scan. Projection
    // ALL because the snapshot needs avg_delay_seconds + p95 + on_time_pct +
    // vehicle_count alongside the keys.
    this.routeAggregatesTable.addGlobalSecondaryIndex({
      indexName: 'window_start_iso-index',
      partitionKey: { name: 'window_start_iso', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });
```

At the end of the constructor, alongside the other CfnOutputs, add:
```typescript
    new cdk.CfnOutput(this, 'WeatherCacheTableName', { value: this.weatherCacheTable.tableName });
```

- [ ] **Step 2: Type-check**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cdk/lib/storage-stack.ts
git commit -m "Phase 7a: weather-cache table + window_start_iso GSI on route-aggregates"
```

---

## Task 2: `feature-snapshot` Lambda — pure helpers (TDD)

**Files:**
- Create: `lambdas/feature_snapshot/handler.py` (initial: pure helpers only)
- Create: `lambdas/feature_snapshot/requirements.txt`
- Create: `lambdas/feature_snapshot/tests/test_handler.py`

> Do NOT create `lambdas/feature_snapshot/__init__.py` or `lambdas/feature_snapshot/tests/__init__.py` — per the Phase 6 polish fix.

- [ ] **Step 1: Write the failing tests for the helpers**

Create `lambdas/feature_snapshot/tests/test_handler.py`:
```python
"""Unit tests for the feature-snapshot Lambda — pure helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/feature_snapshot -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lambdas.feature_snapshot'`.

- [ ] **Step 3: Implement the helpers**

Create `lambdas/feature_snapshot/handler.py`:
```python
"""Feature-snapshot Lambda — Phase 7a.

Runs every 5 min on EventBridge. Reads the second-to-last closed 5-min window
from route-aggregates (via the window_start_iso GSI), fetches one Open-Meteo
observation for LA, writes a single gzipped JSON-lines object per cycle to
s3://.../processed-features/year=…/, and upserts the weather observation
to the weather-cache DDB row (so the precompute-predictions Lambda in 7c
doesn't also call Open-Meteo).

Reading the SECOND-to-last closed window guarantees the Aggregation Lambda
(which rewrites the current 5-min bucket every minute until it closes) has
fully settled the row before we snapshot it. Eliminates a read-while-being-
written race.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

WINDOW_MINUTES = 5


def second_to_last_closed_window_iso(now: datetime) -> str:
    """Return the ISO of the window that started ~10 min before `now`,
    floored to a 5-minute boundary. That's the window we snapshot.
    """
    # Floor `now` to the current 5-min boundary (the *open* window's start).
    floored = now.replace(
        minute=(now.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )
    target = floored - timedelta(minutes=2 * WINDOW_MINUTES)
    return target.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_open_meteo_response(body: bytes) -> dict | None:
    """Parse Open-Meteo's /v1/forecast `current` block into our shape, or
    return None on any parse / shape failure (caller treats as missing weather).
    """
    try:
        doc = json.loads(body)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    current = doc.get("current") if isinstance(doc, dict) else None
    if not isinstance(current, dict):
        return None
    if "temperature_2m" not in current or "precipitation" not in current:
        return None
    return {
        "temp_c": current["temperature_2m"],
        "precip_mm": current["precipitation"],
        "observed_at": current.get("time", ""),
    }


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_feature_record(
    agg_row: dict[str, Any],
    weather: dict | None,
    ingested_at_iso: str,
) -> dict[str, Any]:
    """One (route, window) JSON record for the feature store. Weather fields
    are omitted entirely (not nulled) when Open-Meteo was unreachable, so
    Athena can distinguish "weather unknown" from "no rain"."""
    rec: dict[str, Any] = {
        "route_id": agg_row.get("route_id"),
        "window_start_iso": agg_row.get("window_start_iso"),
        "avg_delay_seconds": _to_int(agg_row.get("avg_delay_seconds")),
        "p95_delay_seconds": _to_int(agg_row.get("p95_delay_seconds")),
        "on_time_pct": _to_float(agg_row.get("on_time_pct")),
        "vehicle_count": _to_int(agg_row.get("vehicle_count")),
        "ingested_at": ingested_at_iso,
    }
    if weather is not None:
        rec["temp_c"] = weather.get("temp_c")
        rec["precip_mm"] = weather.get("precip_mm")
        if weather.get("observed_at"):
            rec["weather_observed_at"] = weather["observed_at"]
    return rec
```

Create `lambdas/feature_snapshot/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; HTTP is via urllib.request stdlib.
# No third-party deps.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest lambdas/feature_snapshot -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add lambdas/feature_snapshot/
git commit -m "Phase 7a: feature-snapshot Lambda helpers + tests (TDD)"
```

---

## Task 3: `feature-snapshot` Lambda — handler (orchestration)

**Files:**
- Modify: `lambdas/feature_snapshot/handler.py`
- Modify: `lambdas/feature_snapshot/tests/test_handler.py`

- [ ] **Step 1: Append failing tests for the handler orchestration**

Append to `lambdas/feature_snapshot/tests/test_handler.py`:
```python
from unittest.mock import MagicMock, patch


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
    # Freeze "now" for deterministic window + key.
    fixed_now = datetime(2026, 5, 27, 12, 5, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(handler, "_utcnow", lambda: fixed_now)

    result = handler.lambda_handler({}, MagicMock())

    assert result["ok"] is True
    assert result["window_start_iso"] == "2026-05-27T11:50:00Z"
    assert result["records_written"] == 2

    # Exactly one S3 PUT this cycle.
    assert s3_client.put_object.call_count == 1
    kwargs = s3_client.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "bkt"
    # Key must be partitioned by year/month/day/hour for the snapshotted window.
    assert kwargs["Key"].startswith(
        "processed-features/year=2026/month=05/day=27/hour=11/"
    )
    assert kwargs["Key"].endswith(".jsonl.gz")
    assert kwargs["ContentType"] == "application/x-ndjson"
    assert kwargs["ContentEncoding"] == "gzip"
    # Decompress + count lines.
    import gzip
    decoded = gzip.decompress(kwargs["Body"]).decode("utf-8").splitlines()
    assert len(decoded) == 2
    parsed = [json.loads(line) for line in decoded]
    assert {p["route_id"] for p in parsed} == {"720", "33"}
    # Each record carries weather.
    assert all("temp_c" in p for p in parsed)

    # Weather-cache row was upserted with TTL.
    wc_table.put_item.assert_called_once()
    cache_item = wc_table.put_item.call_args.kwargs["Item"]
    assert cache_item["id"] == "la"
    assert cache_item["temp_c"] == 22.4
    assert cache_item["precip_mm"] == 0.0
    assert cache_item["ttl_epoch"] > int(fixed_now.timestamp())


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
    # S3 write still happened.
    assert s3_client.put_object.call_count == 1
    # Cache was NOT upserted (no weather to cache).
    wc_table.put_item.assert_not_called()


def test_lambda_handler_no_rows_for_window_exits_clean(monkeypatch):
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
    # Weather cache still updated — that's useful for the precompute Lambda
    # even on a quiet cycle.
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
    """fetch_weather uses urllib.request; we patch urlopen and assert URL + parsing."""
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/feature_snapshot -v`
Expected: FAIL — `AttributeError: module 'lambdas.feature_snapshot.handler' has no attribute 'lambda_handler'` (and missing `_s3`, `_route_aggregates`, `_weather_cache`, `fetch_weather`, `_utcnow`, env-var globals).

- [ ] **Step 3: Implement the orchestration**

Add to the top of `lambdas/feature_snapshot/handler.py`, alongside the existing imports (keep what's already there):
```python
import gzip
import os
import urllib.parse
import urllib.request
from uuid import uuid4

import boto3

ROUTE_AGGREGATES_TABLE = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
ROUTE_AGGREGATES_WINDOW_GSI = os.environ.get(
    "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index"
)
WEATHER_CACHE_TABLE = os.environ.get("WEATHER_CACHE_TABLE_NAME", "")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "")
PROCESSED_FEATURES_PREFIX = os.environ.get(
    "PROCESSED_FEATURES_PREFIX", "processed-features"
)
WEATHER_CACHE_TTL_SECONDS = int(os.environ.get("WEATHER_CACHE_TTL_SECONDS", "600"))

# LA Metro service area center, used as the single weather query point.
OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=34.05&longitude=-118.24"
    "&current=temperature_2m,precipitation"
    "&timezone=UTC"
)
OPEN_METEO_TIMEOUT_SECONDS = 4

_ddb = None
_ra_table = None
_wc_table = None
_s3_client = None


def _ddb_resource():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def _route_aggregates():
    global _ra_table
    if _ra_table is None:
        _ra_table = _ddb_resource().Table(ROUTE_AGGREGATES_TABLE)
    return _ra_table


def _weather_cache():
    global _wc_table
    if _wc_table is None:
        _wc_table = _ddb_resource().Table(WEATHER_CACHE_TABLE)
    return _wc_table


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _utcnow() -> datetime:
    """Indirection so tests can freeze the clock."""
    return datetime.now(timezone.utc)
```

Then add these orchestration functions (after `build_feature_record`, before any handler stub):
```python
def fetch_weather() -> dict | None:
    """Call Open-Meteo for current LA observation. Returns None on any failure
    (caller treats as missing — record still gets written without weather)."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed URL, no user input
            OPEN_METEO_URL, timeout=OPEN_METEO_TIMEOUT_SECONDS
        ) as resp:
            body = resp.read()
    except Exception:
        logger.exception("open_meteo_fetch_failed")
        return None
    return parse_open_meteo_response(body)


def query_window_rows(window_iso: str) -> list[dict[str, Any]]:
    """Read every route's aggregate row for one window via the GSI, draining
    all pages."""
    table = _route_aggregates()
    out: list[dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "IndexName": ROUTE_AGGREGATES_WINDOW_GSI,
            "KeyConditionExpression": "window_start_iso = :w",
            "ExpressionAttributeValues": {":w": window_iso},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.query(**kwargs)
        out.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return out


def upsert_weather_cache(weather: dict, now: datetime) -> None:
    """Single-row cache used by the precompute-predictions Lambda (7c)."""
    _weather_cache().put_item(
        Item={
            "id": "la",
            "temp_c": weather["temp_c"],
            "precip_mm": weather["precip_mm"],
            "observed_at": weather.get("observed_at", ""),
            "cached_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ttl_epoch": int(now.timestamp()) + WEATHER_CACHE_TTL_SECONDS,
        }
    )


def _s3_key_for_window(window_iso: str) -> str:
    """processed-features/year=YYYY/month=MM/day=DD/hour=HH/window=…uuid.jsonl.gz"""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (
        f"{PROCESSED_FEATURES_PREFIX}"
        f"/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"
        f"/window={window_iso}-{uuid4().hex[:8]}.jsonl.gz"
    )


def write_records_to_s3(records: list[dict[str, Any]], window_iso: str) -> str:
    """Gzip + put a single JSONL object. Returns the S3 key."""
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    gz = gzip.compress(body)
    key = _s3_key_for_window(window_iso)
    _s3().put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=key,
        Body=gz,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not (ROUTE_AGGREGATES_TABLE and WEATHER_CACHE_TABLE and ARCHIVE_BUCKET):
        raise RuntimeError(
            "Missing required env: ROUTE_AGGREGATES_TABLE_NAME, "
            "WEATHER_CACHE_TABLE_NAME, ARCHIVE_BUCKET"
        )

    now = _utcnow()
    window_iso = second_to_last_closed_window_iso(now)
    ingested_at_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    weather = fetch_weather()
    rows = query_window_rows(window_iso)

    records = [build_feature_record(r, weather, ingested_at_iso) for r in rows]

    s3_key = None
    if records:
        s3_key = write_records_to_s3(records, window_iso)

    if weather is not None:
        upsert_weather_cache(weather, now)

    log = {
        "ok": True,
        "window_start_iso": window_iso,
        "records_written": len(records),
        "weather_cached": weather is not None,
        "s3_key": s3_key,
    }
    logger.info(json.dumps(log))
    return log
```

- [ ] **Step 4: Run all tests**

Run: `pytest lambdas/feature_snapshot -v`
Expected: PASS (8 helper tests + 5 handler tests = 13).

- [ ] **Step 5: Commit**

```bash
git add lambdas/feature_snapshot/
git commit -m "Phase 7a: feature-snapshot Lambda handler (S3 write + weather-cache upsert)"
```

---

## Task 4: MLStack — Lambda + EventBridge schedule + grants

**Files:**
- Create: `cdk/lib/ml-stack.ts`

> The Glue table comes in Task 5; this task ships the Lambda + schedule + grants only so the stack synths and is reviewable in isolation.

- [ ] **Step 1: Create the MLStack skeleton with the feature-snapshot Lambda**

Create `cdk/lib/ml-stack.ts`:
```typescript
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';

export interface MLStackProps extends cdk.StackProps {
  routeAggregatesTable: dynamodb.ITable;
  weatherCacheTable: dynamodb.ITable;
  archiveBucket: s3.IBucket;
}

/**
 * Phase 7a — ML data foundation.
 *
 * Houses the durable feature-store writer + the Glue table that makes it
 * Athena-queryable. Later phases (7b training pipeline, 7c inference serving)
 * extend this same stack.
 */
export class MLStack extends cdk.Stack {
  public readonly featureSnapshotFn: lambda.Function;

  constructor(scope: Construct, id: string, props: MLStackProps) {
    super(scope, id, props);

    // ---- feature-snapshot Lambda (5-min schedule) ----
    const functionName = 'la-metro-feature-snapshot';
    const logGroup = new logs.LogGroup(this, 'FeatureSnapshotFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.featureSnapshotFn = new lambda.Function(this, 'FeatureSnapshotFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'feature_snapshot', '.build'),
      ),
      memorySize: 256,
      // 30s: GSI query + Open-Meteo (≤4s) + gzip + one S3 PUT is well under
      // 5s in practice. Generous cushion for cold start + slow weather call.
      timeout: cdk.Duration.seconds(30),
      environment: {
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
        ROUTE_AGGREGATES_WINDOW_GSI: 'window_start_iso-index',
        WEATHER_CACHE_TABLE_NAME: props.weatherCacheTable.tableName,
        ARCHIVE_BUCKET: props.archiveBucket.bucketName,
        PROCESSED_FEATURES_PREFIX: 'processed-features',
        WEATHER_CACHE_TTL_SECONDS: '600',
      },
      logGroup,
      description: 'Phase 7a: durable per-(route, window) feature snapshots + weather.',
    });

    // GSI read on route-aggregates (FromIndexName grant requires explicit
    // index resource ARN since CDK's grantReadData only covers the base table).
    props.routeAggregatesTable.grantReadData(this.featureSnapshotFn);
    this.featureSnapshotFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:Query'],
      resources: [`${props.routeAggregatesTable.tableArn}/index/window_start_iso-index`],
    }));

    props.weatherCacheTable.grantWriteData(this.featureSnapshotFn);

    // S3 write scoped to the processed-features/ prefix only — the Lambda has
    // no business touching raw-events/, gtfs-static/, or models/.
    props.archiveBucket.grantPut(this.featureSnapshotFn, 'processed-features/*');

    new events.Rule(this, 'FeatureSnapshotSchedule', {
      ruleName: 'la-metro-feature-snapshot-schedule',
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(this.featureSnapshotFn)],
      description: 'Triggers feature-snapshot every 5 min.',
    });

    new cdk.CfnOutput(this, 'FeatureSnapshotFnName', {
      value: this.featureSnapshotFn.functionName,
    });
  }
}
```

- [ ] **Step 2: Build the asset + type-check**

Run:
```bash
cd /Users/caden/awsProject
scripts/build-lambda.sh feature_snapshot
cd cdk && npx tsc --noEmit
```
Expected: build succeeds; no type errors. (Full synth comes in Task 6 once cdk.ts wires it.)

- [ ] **Step 3: Commit**

```bash
git add cdk/lib/ml-stack.ts
git commit -m "Phase 7a: MLStack — feature-snapshot Lambda + 5-min schedule + grants"
```

---

## Task 5: Glue Database + Table over `processed-features/` (partition projection — no crawler)

**Files:**
- Modify: `cdk/lib/ml-stack.ts`

- [ ] **Step 1: Add imports**

At the top of `cdk/lib/ml-stack.ts`, add:
```typescript
import * as glue from 'aws-cdk-lib/aws-glue';
```

- [ ] **Step 2: Add the Glue DB + Table inside the constructor**

Inside the `MLStack` constructor, AFTER the EventBridge schedule and BEFORE the CfnOutput, add:
```typescript
    // ---- Glue catalog over processed-features/ ----
    // Partition projection avoids needing a crawler: Athena infers the
    // partition values from a date range we declare here. The catalog only
    // stores the table definition; we never pay crawler runtime cost.
    const glueDb = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'la_metro',
        description: 'LA Metro reliability platform — Athena/Glue catalog.',
      },
    });

    const glueTable = new glue.CfnTable(this, 'RouteWindowFeaturesTable', {
      catalogId: this.account,
      databaseName: 'la_metro',
      tableInput: {
        name: 'route_window_features',
        description:
          'Per-(route, window) snapshots written by the feature-snapshot Lambda.',
        tableType: 'EXTERNAL_TABLE',
        parameters: {
          classification: 'json',
          // Partition projection — Athena auto-generates partition values.
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2026,2030',
          'projection.month.type': 'integer',
          'projection.month.range': '1,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '1,31',
          'projection.day.digits': '2',
          'projection.hour.type': 'integer',
          'projection.hour.range': '0,23',
          'projection.hour.digits': '2',
          'storage.location.template':
            `s3://${props.archiveBucket.bucketName}/processed-features/` +
            'year=${year}/month=${month}/day=${day}/hour=${hour}/',
        },
        partitionKeys: [
          { name: 'year', type: 'int' },
          { name: 'month', type: 'int' },
          { name: 'day', type: 'int' },
          { name: 'hour', type: 'int' },
        ],
        storageDescriptor: {
          location: `s3://${props.archiveBucket.bucketName}/processed-features/`,
          inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
          outputFormat:
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.openx.data.jsonserde.JsonSerDe',
            parameters: { 'ignore.malformed.json': 'true' },
          },
          columns: [
            { name: 'route_id', type: 'string' },
            { name: 'window_start_iso', type: 'string' },
            { name: 'avg_delay_seconds', type: 'int' },
            { name: 'p95_delay_seconds', type: 'int' },
            { name: 'on_time_pct', type: 'double' },
            { name: 'vehicle_count', type: 'int' },
            { name: 'temp_c', type: 'double' },
            { name: 'precip_mm', type: 'double' },
            { name: 'weather_observed_at', type: 'string' },
            { name: 'ingested_at', type: 'string' },
          ],
          compressed: true,
        },
      },
    });
    glueTable.addDependency(glueDb);

    new cdk.CfnOutput(this, 'GlueDatabaseName', { value: 'la_metro' });
    new cdk.CfnOutput(this, 'GlueTableName', { value: 'route_window_features' });
```

- [ ] **Step 3: Type-check**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add cdk/lib/ml-stack.ts
git commit -m "Phase 7a: Glue DB + route_window_features table (partition projection)"
```

---

## Task 6: Wire `MLStack` into `cdk/bin/cdk.ts`

**Files:**
- Modify: `cdk/bin/cdk.ts`

- [ ] **Step 1: Import + construct MLStack + add to tagging loop**

In `cdk/bin/cdk.ts`, add the import after the existing stack imports:
```typescript
import { MLStack } from '../lib/ml-stack';
```

Construct `MLStack` after the existing `api`/`frontend` stacks and before the `billing` stack:
```typescript
const ml = new MLStack(app, 'LaMetro-MLStack', {
  env,
  routeAggregatesTable: storage.routeAggregatesTable,
  weatherCacheTable: storage.weatherCacheTable,
  archiveBucket: storage.archiveBucket,
  description: 'Phase 7a: feature-snapshot Lambda + Glue catalog (extended in 7b/7c).',
});
```

Add `ml` to the tagging loop array (currently `[storage, auth, ingestion, processing, api, frontend, websocket, billing]`) so it becomes:
```typescript
for (const stack of [storage, auth, ingestion, processing, api, frontend, websocket, billing, ml]) {
```

- [ ] **Step 2: Full local build + whole-app synth**

Run:
```bash
cd /Users/caden/awsProject
for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot; do
  scripts/build-lambda.sh "$d"
done
cd cdk && npx tsc --noEmit && npx cdk synth --quiet
```
Expected: all 9 stacks synth (including `LaMetro-MLStack`); no missing-asset errors.

- [ ] **Step 3: Commit**

```bash
git add cdk/bin/cdk.ts
git commit -m "Phase 7a: wire MLStack into CDK app"
```

---

## Task 7: Add `feature_snapshot` to the CI build loop

**Files:**
- Modify: `.github/workflows/pr-checks.yml`

- [ ] **Step 1: Add the new lambda to the build list**

In `.github/workflows/pr-checks.yml`, in the `cdk` job's "Build Lambda assets" step, append `feature_snapshot` to the loop's directory list:
```yaml
          for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot; do
            scripts/build-lambda.sh "$d"
          done
```

- [ ] **Step 2: Full Python suite (sanity)**

Run: `pytest lambdas/ -q`
Expected: previous 130 + 13 new feature_snapshot tests = **143 passed**.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pr-checks.yml
git commit -m "Phase 7a: add feature_snapshot to CI build list"
```

---

## Task 8: `ml/bootstrap.py` — synthetic data seeder (so 7b/7c are demoable on Day 1)

**Files:**
- Create: `ml/bootstrap.py`
- Create: `ml/tests/test_bootstrap.py`

- [ ] **Step 1: Write the failing tests**

Create `ml/tests/test_bootstrap.py`:
```python
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
    # Records are tagged with the right routes + monotonic window times.
    by_route: dict[str, list] = {}
    for r in records:
        by_route.setdefault(r["route_id"], []).append(r["window_start_iso"])
    assert set(by_route.keys()) == {"720", "33", "2"}
    for route_id, isos in by_route.items():
        assert isos == sorted(isos)


def test_partition_key_for_window():
    iso = "2026-05-20T08:35:00Z"
    assert bootstrap.partition_key_for(iso) == "year=2026/month=05/day=20/hour=08"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest ml -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ml'` (and `ml.bootstrap`).

- [ ] **Step 3: Implement**

Create `ml/bootstrap.py`:
```python
"""Synthetic-data seeder for Phase 7a's feature store.

Standalone script (not Lambda). Usage:
    python -m ml.bootstrap --bucket la-metro-archive-dev --days 7

Generates plausible per-(route, window) records — with hour-of-day +
weekday/weekend patterns and Gaussian noise — and uploads them to the same
S3 prefix the live feature-snapshot Lambda writes to. Lets 7b/7c be built
and demoed against a populated feature store without waiting weeks for
real data to accumulate. The bootstrap rows simply age out of the training
window naturally once real data accumulates past the same date.
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator

# A handful of routes representative of LA Metro's mix (frequent rapids,
# locals, rail). Replace with the real route list in production runs.
DEFAULT_ROUTES = ["720", "754", "2", "33", "212", "910"]

# Hour-of-day delay baseline (UTC-ish, will be shifted by tz in caller).
# Rush hours (15-18 UTC ≈ morning LA, 23-02 UTC ≈ evening LA) elevated.
def _baseline_delay_seconds(hour_utc: int, is_weekend: bool) -> float:
    rush_morning = 15 <= hour_utc <= 18
    rush_evening = hour_utc in (23, 0, 1, 2)
    if is_weekend:
        base = 30.0
    elif rush_morning or rush_evening:
        base = 180.0
    else:
        base = 60.0
    return base


def synthetic_record(
    route_id: str, window_start: datetime, *, seed: int
) -> dict:
    """Deterministic per-seed synthetic record. Same shape as the live writer."""
    rng = random.Random(seed)
    hour = window_start.hour
    is_weekend = window_start.weekday() >= 5
    base = _baseline_delay_seconds(hour, is_weekend)
    avg = max(0, int(rng.gauss(base, 45)))
    p95 = avg + max(0, int(rng.gauss(120, 60)))
    on_time_pct = max(0.0, min(100.0, 100.0 - avg / 6.0))
    vehicle_count = max(1, int(rng.gauss(8 if not is_weekend else 4, 3)))
    temp_c = round(rng.gauss(20.0, 5.0), 1)
    precip_mm = round(max(0.0, rng.gauss(0.0, 0.5)), 2)
    return {
        "route_id": route_id,
        "window_start_iso": window_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "avg_delay_seconds": avg,
        "p95_delay_seconds": p95,
        "on_time_pct": round(on_time_pct, 1),
        "vehicle_count": vehicle_count,
        "temp_c": temp_c,
        "precip_mm": precip_mm,
        "weather_observed_at": window_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate_windows(
    start: datetime, end: datetime, *, window_minutes: int = 5
) -> Iterator[datetime]:
    """Yield window_start datetimes from `start` (inclusive) to `end` (exclusive)."""
    cur = start
    step = timedelta(minutes=window_minutes)
    while cur < end:
        yield cur
        cur += step


def records_for(
    routes: Iterable[str],
    start: datetime,
    end: datetime,
    *,
    base_seed: int = 0,
) -> Iterator[dict]:
    """All (route, window) records over the range. Stable seed per (route, window)."""
    seed_idx = base_seed
    for w in generate_windows(start, end):
        for route_id in routes:
            yield synthetic_record(route_id, w, seed=seed_idx)
            seed_idx += 1


def partition_key_for(window_iso: str) -> str:
    """Hive-style prefix that matches what feature-snapshot writes."""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"


def _upload_partition(
    s3_client, bucket: str, key_prefix: str, partition_prefix: str, records: list[dict]
) -> str:
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    gz = gzip.compress(body)
    key = (
        f"{key_prefix}/{partition_prefix}/window={records[0]['window_start_iso']}"
        f"-bootstrap.jsonl.gz"
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=gz,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


def main(argv: list[str] | None = None) -> int:
    import boto3  # imported lazily so the unit tests don't need boto3

    parser = argparse.ArgumentParser(description="Seed Phase 7a synthetic feature data.")
    parser.add_argument("--bucket", required=True, help="Archive bucket name.")
    parser.add_argument(
        "--prefix", default="processed-features",
        help="S3 key prefix (default: processed-features).",
    )
    parser.add_argument("--days", type=int, default=7, help="Days of data to generate.")
    parser.add_argument(
        "--routes", nargs="+", default=DEFAULT_ROUTES,
        help="Route IDs to generate.",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="ISO end timestamp (UTC, exclusive). Defaults to now.",
    )
    args = parser.parse_args(argv)

    end = (
        datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc).replace(second=0, microsecond=0)
    )
    start = end - timedelta(days=args.days)

    s3 = boto3.client("s3")
    # Bucket records by partition, then one PUT per (partition, window).
    buf: dict[str, list[dict]] = {}
    for rec in records_for(args.routes, start, end):
        pkey = partition_key_for(rec["window_start_iso"])
        cohort_key = f"{pkey}|{rec['window_start_iso']}"
        buf.setdefault(cohort_key, []).append(rec)

    written = 0
    for cohort_key, recs in buf.items():
        pkey, _ = cohort_key.split("|", 1)
        _upload_partition(s3, args.bucket, args.prefix, pkey, recs)
        written += 1
    print(f"Uploaded {written} cohort objects to s3://{args.bucket}/{args.prefix}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests**

Run: `pytest ml -v`
Expected: PASS (6 tests).

> If pytest fails to discover `ml/tests/test_bootstrap.py`, confirm `ml/` is reachable (the root `pyproject.toml` `pythonpath = ["."]` covers it). Do NOT add `__init__.py` to `ml/` or `ml/tests/` — same collision risk as the lambdas.

- [ ] **Step 5: Add `ml/` to pytest testpaths so it runs in the suite**

In `pyproject.toml`, the existing `testpaths` line is `testpaths = ["lambdas"]`. Change it to:
```toml
testpaths = ["lambdas", "ml"]
```

Verify the full suite picks up the new tests:
```bash
pytest -q
```
Expected: previous 143 + 6 new = **149 passed**.

- [ ] **Step 6: Commit**

```bash
git add ml/ pyproject.toml
git commit -m "Phase 7a: ml/bootstrap.py synthetic data seeder + pytest path"
```

---

## Deployment & manual verification (after all tasks)

Run once, manually, with AWS credentials:

- [ ] `cd cdk && npx cdk deploy LaMetro-StorageStack LaMetro-MLStack` (StorageStack first so the GSI exists; MLStack second so its Lambda has a target).
- [ ] Wait ~10 min, then check CloudWatch logs `/aws/lambda/la-metro-feature-snapshot` — expect log lines like `{"ok": true, "window_start_iso": "...", "records_written": N, ...}`.
- [ ] Verify `s3://<archive>/processed-features/year=…/` is being populated.
- [ ] Verify the `weather-cache` table has the `id="la"` row updating each cycle.
- [ ] In Athena, run `SELECT COUNT(*) FROM la_metro.route_window_features WHERE year=2026 AND month=5;` to confirm the Glue table reads the data.
- [ ] (Optional, before 7b is ready) Run `python -m ml.bootstrap --bucket <archive> --days 7` to seed synthetic backfill so 7b has training data on Day 1.

---

## Self-review notes (author)

- **Spec coverage:** 7a section of the spec maps task-for-task: weather-cache table (Task 1), feature-snapshot Lambda (Tasks 2–3), MLStack with EventBridge schedule + grants (Task 4), Glue DB/table (Task 5), CDK wiring (Task 6), CI (Task 7), bootstrap script (Task 8). The `window_start_iso-index` GSI on `route-aggregates` is an addition the spec implied (the snapshot Lambda's "scans `route-aggregates` for the chosen `window_start_iso`") but didn't explicitly call out as a schema change; included here because a scan would blow the budget.
- **Placeholder check:** none.
- **Type consistency:** `WEATHER_CACHE_TTL_SECONDS` matches the spec's "10-min TTL" (600 = 10 min). `ROUTE_AGGREGATES_WINDOW_GSI` name `window_start_iso-index` matches the StorageStack definition. `processed-features/` prefix consistent across Lambda env, Glue table location.template, IAM grant scope, and bootstrap script default.
