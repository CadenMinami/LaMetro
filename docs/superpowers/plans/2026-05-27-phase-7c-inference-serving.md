# Phase 7c — Inference Serving + Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve route-level delay predictions on the dashboard. A SageMaker Serverless Inference endpoint hosts the trained XGBoost; a 5-min `precompute-predictions` Lambda invokes it for every route and writes results to a `route-predictions` DynamoDB table; the public API serves `GET /routes/{routeId}/prediction` directly from DynamoDB (no endpoint cold start on the user path). The route detail page gains a trendline card: "currently +X min, predicted +Y min ↑".

**Architecture:** The endpoint is created/refreshed by an `update_endpoint` Lambda added to the nightly Step Functions pipeline immediately after `promote_model` (each promotion produces a fresh `SageMaker:Model` + `EndpointConfig` + `UpdateEndpoint`). The precompute Lambda runs every 5 min, reads recent lags from `route-aggregates`, reads cached weather from `weather-cache`, calls the endpoint per route, and upserts a row per route into `route-predictions` (PK = route_id, 15-min TTL). The existing `query-api` Lambda gains one new resource: `/routes/{routeId}/prediction`, returning the row or 404. The frontend reads it and renders the trendline.

**Tech Stack:** AWS CDK v2, Python 3.12 Lambdas, pytest + `unittest.mock`, Next.js 14 (frontend), SageMaker Serverless Inference + Model + EndpointConfig CFN resources.

**Spec:** `docs/superpowers/specs/2026-05-27-phase-7-delay-prediction-design.md` (sub-piece 7c)

**Prerequisites:** Phase 7b deployed AND has run successfully at least once, so `s3://<archive>/models/current/model.tar.gz` exists. Without that, the SageMaker Model resource fails to create on first CDK deploy.

---

## Refinement note

This plan was written together with the 7a and 7b plans. Two areas to verify before executing:
1. **The exact feature-vector layout the endpoint expects.** The training CSV's column order from `feature_extraction.sql` is `label, route_code, hour_of_day, day_of_week, lag1, lag2, lag3, temp_c, precip_mm`. The endpoint will receive features in that same order (sans `label`) at inference time. If the SQL's column order is changed in 7b's refinement, update `assemble_feature_csv` here to match.
2. **The route-code derivation.** 7b derives `route_code` from a hash of `route_id`. The precompute Lambda must use the same derivation. Encapsulated in `derive_route_code()` here; cross-check vs 7b's chosen Athena function.

---

## Conventions (same as 7a/7b)

- Tests at `lambdas/<name>/tests/test_handler.py`; MagicMock + monkeypatch; **no `__init__.py`** in lambda or tests dirs.
- CDK verify: `npx tsc --noEmit` + `npx cdk synth --quiet`.
- New lambdas added to CI build loop in `.github/workflows/pr-checks.yml`.
- No `Co-Authored-By: Claude` trailer.

---

## File map

| File | Status | Responsibility |
|---|---|---|
| `cdk/lib/storage-stack.ts` | modify | Add `route-predictions` DDB table |
| `cdk/lib/ml-stack.ts` | modify | SageMaker Model + EndpointConfig + Endpoint; precompute Lambda + schedule; `update_endpoint` Lambda; insert new state in state-machine after Promote |
| `cdk/lib/api-stack.ts` | modify | Add `/routes/{routeId}/prediction` resource (public, served by existing query-api) |
| `cdk/bin/cdk.ts` | modify | Pass `routePredictionsTable` to MLStack and ApiStack |
| `lambdas/precompute_predictions/handler.py` | create | The precompute Lambda |
| `lambdas/precompute_predictions/requirements.txt` | create | Comment-only |
| `lambdas/precompute_predictions/tests/test_handler.py` | create | TDD tests |
| `lambdas/update_endpoint/handler.py` | create | The endpoint-refresh Lambda |
| `lambdas/update_endpoint/requirements.txt` | create | Comment-only |
| `lambdas/update_endpoint/tests/test_handler.py` | create | TDD tests |
| `lambdas/query_api/handler.py` | modify | Add `handle_route_prediction(event)` + dispatch |
| `lambdas/query_api/tests/test_handler.py` | modify | Tests for prediction route (200 + 404) |
| `frontend/lib/api.ts` | modify | Add `fetchRoutePrediction(routeId)` + typed response |
| `frontend/app/route/page.tsx` | modify | Trendline card |
| `.github/workflows/pr-checks.yml` | modify | Add `precompute_predictions`, `update_endpoint` to build loop |

---

## Task 1: StorageStack — `route-predictions` table

**Files:**
- Modify: `cdk/lib/storage-stack.ts`

- [ ] **Step 1: Add the field + construct**

Add a public field:
```typescript
  public readonly routePredictionsTable: dynamodb.Table;
```

Inside the constructor, after `weatherCacheTable` (added in 7a), add:
```typescript
    // Phase 7c: one row per route, overwritten every 5 min by the
    // precompute-predictions Lambda. The query API reads from here so user
    // requests never pay a SageMaker endpoint cold start.
    this.routePredictionsTable = new dynamodb.Table(this, 'RoutePredictionsTable', {
      tableName: 'la-metro-route-predictions',
      partitionKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',  // 15-min TTL — stale rows can't linger
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
```

Add the CfnOutput next to the others:
```typescript
    new cdk.CfnOutput(this, 'RoutePredictionsTableName', {
      value: this.routePredictionsTable.tableName,
    });
```

- [ ] **Step 2: Type-check + commit**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors.

```bash
git add cdk/lib/storage-stack.ts
git commit -m "Phase 7c: route-predictions DynamoDB table"
```

---

## Task 2: `precompute_predictions` Lambda — pure helpers (TDD)

**Files:**
- Create: `lambdas/precompute_predictions/handler.py` (helpers only)
- Create: `lambdas/precompute_predictions/requirements.txt`
- Create: `lambdas/precompute_predictions/tests/test_handler.py`

> Do NOT create `__init__.py` in either dir.

- [ ] **Step 1: Write the failing helper tests**

Create `lambdas/precompute_predictions/tests/test_handler.py`:
```python
"""Unit tests for the precompute-predictions Lambda — pure helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from lambdas.precompute_predictions import handler


def test_derive_route_code_is_deterministic_and_bounded():
    # Same input → same code; output in [0, 1000) to match Athena's % 1000.
    a = handler.derive_route_code("720")
    b = handler.derive_route_code("720")
    c = handler.derive_route_code("33")
    assert a == b
    assert a != c
    assert 0 <= a < 1000
    assert 0 <= c < 1000


def test_pick_recent_lags_returns_3_most_recent_avg_delays():
    # Items are pre-sorted newest first by the GSI query.
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
    # Order MUST match the training SQL: route_code, hour_of_day, day_of_week,
    # lag1, lag2, lag3, temp_c, precip_mm
    csv = handler.assemble_feature_csv(
        route_code=42,
        hour_of_day=8,
        day_of_week=3,
        lags=[120, 90, 60],
        temp_c=18.5,
        precip_mm=0.0,
    )
    assert csv == "42,8,3,120,90,60,18.5,0.0"


def test_assemble_feature_csv_handles_null_weather():
    # If weather is missing (cache empty), substitute 0.0 to match training
    # COALESCE behavior in the Athena SQL.
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/precompute_predictions -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement the helpers**

Create `lambdas/precompute_predictions/handler.py`:
```python
"""Precompute-predictions Lambda — Phase 7c.

Runs every 5 min on EventBridge. For each route with recent aggregate data,
assembles the live feature vector, calls the SageMaker Serverless endpoint,
and writes the prediction to the route-predictions DynamoDB table. The
public query API reads from that table, so user requests are instant.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def derive_route_code(route_id: str) -> int:
    """Stable bucket in [0, 1000) — must match 7b's Athena route_code derivation.

    Athena's SQL uses `abs(checksum(cast(route_id as varbinary))) % 1000`;
    Python doesn't have an exact equivalent, so we use a deterministic hash
    of the same modulus. Cross-check: if the SQL is changed during 7b's
    refinement to use a different derivation, change this function to match.
    """
    h = hashlib.sha256(route_id.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % 1000


def _to_int_or_none(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def pick_recent_lags(rows: list[dict[str, Any]]) -> list[int]:
    """Return the 3 most recent avg_delay_seconds (newest first). Rows are
    expected pre-sorted newest first by the GSI query (ScanIndexForward=False).
    Missing/null values are skipped; if fewer than 3 valid lags exist, the
    result is padded with 0 (matching the COALESCE behavior at training)."""
    lags: list[int] = []
    for r in rows:
        if len(lags) == 3:
            break
        v = _to_int_or_none(r.get("avg_delay_seconds"))
        if v is None:
            continue
        lags.append(v)
    while len(lags) < 3:
        lags.append(0)
    return lags


def assemble_feature_csv(
    *,
    route_code: int,
    hour_of_day: int,
    day_of_week: int,
    lags: list[int],
    temp_c: float | None,
    precip_mm: float | None,
) -> str:
    """Single CSV line in the column order the trained XGBoost expects.

    MUST match the training SQL's projection (sans label):
      route_code, hour_of_day, day_of_week, lag1, lag2, lag3, temp_c, precip_mm
    """
    t = 0.0 if temp_c is None else float(temp_c)
    p = 0.0 if precip_mm is None else float(precip_mm)
    return (
        f"{route_code},{hour_of_day},{day_of_week},"
        f"{lags[0]},{lags[1]},{lags[2]},{t},{p}"
    )


def build_prediction_item(
    *,
    route_id: str,
    predicted: int,
    current: int,
    model_version: str,
    window_start_iso: str,
    as_of: datetime,
    ttl_seconds: int = 900,
) -> dict[str, Any]:
    return {
        "route_id": route_id,
        "predicted_next_window_avg_delay_seconds": predicted,
        "current_avg_delay_seconds": current,
        "model_version": model_version,
        "window_start_iso": window_start_iso,
        "as_of": as_of.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ttl_epoch": int(as_of.timestamp()) + ttl_seconds,
    }
```

Create `lambdas/precompute_predictions/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [ ] **Step 4: Run tests + commit**

Run: `pytest lambdas/precompute_predictions -v`
Expected: PASS (7 tests).

```bash
git add lambdas/precompute_predictions/
git commit -m "Phase 7c: precompute-predictions Lambda helpers (TDD)"
```

---

## Task 3: `precompute_predictions` Lambda — handler

**Files:**
- Modify: `lambdas/precompute_predictions/handler.py`
- Modify: `lambdas/precompute_predictions/tests/test_handler.py`

- [ ] **Step 1: Append failing tests for the handler**

Append to the test file:
```python
from unittest.mock import MagicMock


def _agg_query_response(route_id: str, base_avg: int):
    return {
        "Items": [
            {"route_id": route_id, "window_start_iso": "2026-05-27T12:05:00Z",
             "avg_delay_seconds": Decimal(str(base_avg + 30))},
            {"route_id": route_id, "window_start_iso": "2026-05-27T12:00:00Z",
             "avg_delay_seconds": Decimal(str(base_avg + 15))},
            {"route_id": route_id, "window_start_iso": "2026-05-27T11:55:00Z",
             "avg_delay_seconds": Decimal(str(base_avg))},
        ],
    }


def test_lambda_handler_predicts_each_route_and_writes_to_ddb(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_ROUTE_GSI", "route_id-last_updated-index")
    monkeypatch.setattr(handler, "ROUTE_PREDICTIONS_TABLE", "rp")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "MODELS_PREFIX_URI", "s3://bkt/models")
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "la-metro-delay-predictor")

    ra = MagicMock()
    # Two routes have recent data; queried via GSI per route.
    ra.query.side_effect = [
        _agg_query_response("720", 60),
        _agg_query_response("33", 20),
    ]
    rp = MagicMock()
    wc = MagicMock()
    wc.get_item.return_value = {"Item": {"temp_c": Decimal("22.4"), "precip_mm": Decimal("0.0")}}
    sm = MagicMock()
    # Endpoint returns a single predicted scalar as text (built-in XGBoost
    # default output format).
    sm.invoke_endpoint.side_effect = [
        {"Body": MagicMock(read=lambda: b"125.7")},
        {"Body": MagicMock(read=lambda: b"45.2")},
    ]
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"promoted_version": "v=2026-06-15"}).encode())
    }

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra)
    monkeypatch.setattr(handler, "_route_predictions", lambda: rp)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc)
    monkeypatch.setattr(handler, "_sagemaker_runtime", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(handler, "list_active_routes", lambda: ["720", "33"])
    fixed_now = datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(handler, "_utcnow", lambda: fixed_now)

    result = handler.lambda_handler({}, MagicMock())
    assert result["routes_attempted"] == 2
    assert result["predictions_written"] == 2
    assert result["model_version"] == "v=2026-06-15"

    assert sm.invoke_endpoint.call_count == 2
    # Verify endpoint name + ContentType.
    first_call = sm.invoke_endpoint.call_args_list[0].kwargs
    assert first_call["EndpointName"] == "la-metro-delay-predictor"
    assert first_call["ContentType"] == "text/csv"

    # Verify DDB writes.
    assert rp.put_item.call_count == 2
    items_by_route = {
        c.kwargs["Item"]["route_id"]: c.kwargs["Item"]
        for c in rp.put_item.call_args_list
    }
    assert set(items_by_route.keys()) == {"720", "33"}
    assert items_by_route["720"]["predicted_next_window_avg_delay_seconds"] == 126
    # current_avg = the most recent lag (60+30 = 90 for 720).
    assert items_by_route["720"]["current_avg_delay_seconds"] == 90


def test_lambda_handler_per_route_failure_does_not_block_cycle(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_ROUTE_GSI", "route_id-last_updated-index")
    monkeypatch.setattr(handler, "ROUTE_PREDICTIONS_TABLE", "rp")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "MODELS_PREFIX_URI", "s3://bkt/models")
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "la-metro-delay-predictor")

    ra = MagicMock()
    ra.query.side_effect = [
        _agg_query_response("720", 60),
        _agg_query_response("33", 20),
    ]
    rp = MagicMock()
    wc = MagicMock()
    wc.get_item.return_value = {"Item": {"temp_c": Decimal("22.4"), "precip_mm": Decimal("0.0")}}
    sm = MagicMock()
    # First route fails (endpoint throws); second succeeds.
    sm.invoke_endpoint.side_effect = [
        RuntimeError("endpoint cold-start timeout"),
        {"Body": MagicMock(read=lambda: b"45.2")},
    ]
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"promoted_version": "v=2026-06-15"}).encode())
    }

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra)
    monkeypatch.setattr(handler, "_route_predictions", lambda: rp)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc)
    monkeypatch.setattr(handler, "_sagemaker_runtime", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(handler, "list_active_routes", lambda: ["720", "33"])
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc),
    )

    result = handler.lambda_handler({}, MagicMock())
    assert result["routes_attempted"] == 2
    assert result["predictions_written"] == 1
    assert result["per_route_failures"] == 1
    assert rp.put_item.call_count == 1  # only the successful route


def test_lambda_handler_missing_endpoint_envvar_raises(monkeypatch):
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "")
    import pytest as _pytest
    with _pytest.raises(RuntimeError):
        handler.lambda_handler({}, MagicMock())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/precompute_predictions -v`
Expected: FAIL on the new tests — handler isn't implemented yet.

- [ ] **Step 3: Implement the handler**

Append to `lambdas/precompute_predictions/handler.py`:
```python
import boto3
from boto3.dynamodb.conditions import Key

ROUTE_AGGREGATES_TABLE = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
ROUTE_AGGREGATES_ROUTE_GSI = os.environ.get(
    "ROUTE_AGGREGATES_ROUTE_GSI", "route_id-last_updated-index"
)
ROUTE_PREDICTIONS_TABLE = os.environ.get("ROUTE_PREDICTIONS_TABLE_NAME", "")
WEATHER_CACHE_TABLE = os.environ.get("WEATHER_CACHE_TABLE_NAME", "")
MODELS_PREFIX_URI = os.environ.get("MODELS_PREFIX_URI", "")  # s3://bkt/models
SAGEMAKER_ENDPOINT_NAME = os.environ.get("SAGEMAKER_ENDPOINT_NAME", "")
PREDICTION_TTL_SECONDS = int(os.environ.get("PREDICTION_TTL_SECONDS", "900"))

_ddb = None
_ra_table = None
_rp_table = None
_wc_table = None
_sm_runtime = None
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


def _route_predictions():
    global _rp_table
    if _rp_table is None:
        _rp_table = _ddb_resource().Table(ROUTE_PREDICTIONS_TABLE)
    return _rp_table


def _weather_cache():
    global _wc_table
    if _wc_table is None:
        _wc_table = _ddb_resource().Table(WEATHER_CACHE_TABLE)
    return _wc_table


def _sagemaker_runtime():
    global _sm_runtime
    if _sm_runtime is None:
        _sm_runtime = boto3.client("sagemaker-runtime")
    return _sm_runtime


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def list_active_routes() -> list[str]:
    """Return the set of route_ids that have written to route-aggregates in
    the recent past. v1: scan-with-projection on route-aggregates' GSI for
    distinct route_ids. For our ~150-route scale this is acceptably cheap;
    if it ever becomes hot, switch to a maintained 'active_routes' record."""
    table = _route_aggregates()
    seen: set[str] = set()
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "IndexName": ROUTE_AGGREGATES_ROUTE_GSI,
            "ProjectionExpression": "route_id",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        for it in resp.get("Items", []):
            r = it.get("route_id")
            if r:
                seen.add(r)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return sorted(seen)


def _fetch_recent_lags_for_route(route_id: str) -> list[dict[str, Any]]:
    """Query route-aggregates' route_id GSI for the most recent rows for one
    route (ScanIndexForward=False on last_updated)."""
    table = _route_aggregates()
    resp = table.query(
        IndexName=ROUTE_AGGREGATES_ROUTE_GSI,
        KeyConditionExpression=Key("route_id").eq(route_id),
        ScanIndexForward=False,
        Limit=4,  # need 3 valid lags; pull 4 in case one is null
    )
    return resp.get("Items", [])


def _read_weather_cache() -> tuple[float | None, float | None]:
    resp = _weather_cache().get_item(Key={"id": "la"})
    item = resp.get("Item")
    if not item:
        return None, None
    t = item.get("temp_c")
    p = item.get("precip_mm")
    try:
        t = None if t is None else float(t)
    except (TypeError, ValueError):
        t = None
    try:
        p = None if p is None else float(p)
    except (TypeError, ValueError):
        p = None
    return t, p


def _read_deployed_model_version() -> str:
    """Read promoted_version from s3://<archive>/models/current/metrics.json.
    Returns "unknown" on any failure (predictions are still useful even if we
    can't tag them with a version)."""
    if not MODELS_PREFIX_URI:
        return "unknown"
    from urllib.parse import urlparse
    p = urlparse(MODELS_PREFIX_URI.rstrip("/"))
    bucket, key_prefix = p.netloc, p.path.lstrip("/")
    try:
        body = _s3().get_object(
            Bucket=bucket, Key=f"{key_prefix}/current/metrics.json"
        )["Body"].read()
        return json.loads(body).get("promoted_version", "unknown")
    except Exception:
        logger.exception("could not read deployed model version")
        return "unknown"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not SAGEMAKER_ENDPOINT_NAME:
        raise RuntimeError("SAGEMAKER_ENDPOINT_NAME env not set")

    now = _utcnow()
    temp_c, precip_mm = _read_weather_cache()
    model_version = _read_deployed_model_version()
    routes = list_active_routes()

    sm = _sagemaker_runtime()
    rp = _route_predictions()

    attempted = 0
    written = 0
    failures = 0
    for route_id in routes:
        attempted += 1
        try:
            rows = _fetch_recent_lags_for_route(route_id)
            lags = pick_recent_lags(rows)
            current = lags[0]
            csv = assemble_feature_csv(
                route_code=derive_route_code(route_id),
                hour_of_day=now.hour,
                day_of_week=now.isoweekday(),  # 1 = Mon … 7 = Sun
                lags=lags,
                temp_c=temp_c,
                precip_mm=precip_mm,
            )
            resp = sm.invoke_endpoint(
                EndpointName=SAGEMAKER_ENDPOINT_NAME,
                ContentType="text/csv",
                Body=csv.encode("utf-8"),
            )
            predicted_raw = resp["Body"].read().decode("utf-8").strip()
            predicted = int(round(float(predicted_raw)))
            window_iso = rows[0].get("window_start_iso", "") if rows else ""
            rp.put_item(
                Item=build_prediction_item(
                    route_id=route_id,
                    predicted=predicted,
                    current=current,
                    model_version=model_version,
                    window_start_iso=window_iso,
                    as_of=now,
                    ttl_seconds=PREDICTION_TTL_SECONDS,
                )
            )
            written += 1
        except Exception:
            # Per-route isolation — one bad route never blocks the cycle.
            failures += 1
            logger.exception("predict_failed route=%s", route_id)

    out = {
        "routes_attempted": attempted,
        "predictions_written": written,
        "per_route_failures": failures,
        "model_version": model_version,
    }
    logger.info(str(out))
    return out
```

- [ ] **Step 4: Run tests + commit**

Run: `pytest lambdas/precompute_predictions -v`
Expected: PASS (7 helpers + 3 handler = 10).

```bash
git add lambdas/precompute_predictions/
git commit -m "Phase 7c: precompute-predictions Lambda handler (per-route invoke + DDB write)"
```

---

## Task 4: `update_endpoint` Lambda — refresh SageMaker endpoint to a new model

**Files:**
- Create: `lambdas/update_endpoint/handler.py`
- Create: `lambdas/update_endpoint/requirements.txt`
- Create: `lambdas/update_endpoint/tests/test_handler.py`

- [ ] **Step 1: Write the failing tests**

Create the test file:
```python
"""Unit tests for the update-endpoint Lambda (Phase 7c)."""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.update_endpoint import handler


def test_resource_names_use_promoted_version():
    names = handler.resource_names("v=2026-06-15")
    assert names["model_name"] == "la-metro-delay-predictor-v-2026-06-15"
    assert names["endpoint_config_name"] == "la-metro-delay-predictor-cfg-v-2026-06-15"


def test_lambda_handler_creates_model_config_and_updates_endpoint(monkeypatch):
    sm = MagicMock()
    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "ENDPOINT_NAME", "la-metro-delay-predictor")
    monkeypatch.setattr(handler, "TRAINING_IMAGE", "img/xgboost:1.7-1")
    monkeypatch.setattr(handler, "EXECUTION_ROLE_ARN", "arn:aws:iam::123:role/SageMakerExec")
    monkeypatch.setattr(handler, "MEMORY_SIZE_MB", "1024")
    monkeypatch.setattr(handler, "MAX_CONCURRENCY", "5")

    event = {
        "promoted_version": "v=2026-06-15",
        "current_model_uri": "s3://bkt/models/current/model.tar.gz",
    }
    out = handler.lambda_handler(event, MagicMock())
    assert out["updated_endpoint"] == "la-metro-delay-predictor"

    sm.create_model.assert_called_once()
    cm = sm.create_model.call_args.kwargs
    assert cm["ModelName"] == "la-metro-delay-predictor-v-2026-06-15"
    assert cm["PrimaryContainer"]["Image"] == "img/xgboost:1.7-1"
    assert cm["PrimaryContainer"]["ModelDataUrl"] == "s3://bkt/models/current/model.tar.gz"
    assert cm["ExecutionRoleArn"] == "arn:aws:iam::123:role/SageMakerExec"

    sm.create_endpoint_config.assert_called_once()
    ec = sm.create_endpoint_config.call_args.kwargs
    assert ec["EndpointConfigName"] == "la-metro-delay-predictor-cfg-v-2026-06-15"
    variant = ec["ProductionVariants"][0]
    assert variant["ModelName"] == "la-metro-delay-predictor-v-2026-06-15"
    assert variant["ServerlessConfig"]["MemorySizeInMB"] == 1024
    assert variant["ServerlessConfig"]["MaxConcurrency"] == 5

    sm.update_endpoint.assert_called_once_with(
        EndpointName="la-metro-delay-predictor",
        EndpointConfigName="la-metro-delay-predictor-cfg-v-2026-06-15",
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/update_endpoint -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

Create `lambdas/update_endpoint/handler.py`:
```python
"""Update SageMaker endpoint — Phase 7c.

Called by the Step Functions pipeline immediately after promote_model. Creates
a new SageMaker Model (timestamped by promoted_version) + EndpointConfig and
calls UpdateEndpoint so the live endpoint serves the freshly-promoted model.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_NAME = os.environ.get("SAGEMAKER_ENDPOINT_NAME", "")
TRAINING_IMAGE = os.environ.get("TRAINING_IMAGE", "")
EXECUTION_ROLE_ARN = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN", "")
MEMORY_SIZE_MB = os.environ.get("ENDPOINT_MEMORY_MB", "1024")
MAX_CONCURRENCY = os.environ.get("ENDPOINT_MAX_CONCURRENCY", "5")

_sm = None


def _sagemaker():
    global _sm
    if _sm is None:
        _sm = boto3.client("sagemaker")
    return _sm


def _sanitize(name: str) -> str:
    """SageMaker resource names allow [a-zA-Z0-9-]; replace = and . in a
    version like `v=2026-06-15`."""
    return name.replace("=", "-").replace(".", "-").replace("_", "-")


def resource_names(promoted_version: str) -> dict[str, str]:
    v = _sanitize(promoted_version)
    return {
        "model_name": f"la-metro-delay-predictor-{v}",
        "endpoint_config_name": f"la-metro-delay-predictor-cfg-{v}",
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not (ENDPOINT_NAME and TRAINING_IMAGE and EXECUTION_ROLE_ARN):
        raise RuntimeError(
            "Missing env: SAGEMAKER_ENDPOINT_NAME / TRAINING_IMAGE / "
            "SAGEMAKER_EXECUTION_ROLE_ARN"
        )

    promoted_version = event["promoted_version"]
    model_uri = event["current_model_uri"]
    names = resource_names(promoted_version)

    sm = _sagemaker()
    sm.create_model(
        ModelName=names["model_name"],
        PrimaryContainer={
            "Image": TRAINING_IMAGE,
            "ModelDataUrl": model_uri,
            "Environment": {"SAGEMAKER_PROGRAM": "inference"},
        },
        ExecutionRoleArn=EXECUTION_ROLE_ARN,
    )
    sm.create_endpoint_config(
        EndpointConfigName=names["endpoint_config_name"],
        ProductionVariants=[{
            "VariantName": "AllTraffic",
            "ModelName": names["model_name"],
            "ServerlessConfig": {
                "MemorySizeInMB": int(MEMORY_SIZE_MB),
                "MaxConcurrency": int(MAX_CONCURRENCY),
            },
        }],
    )
    sm.update_endpoint(
        EndpointName=ENDPOINT_NAME,
        EndpointConfigName=names["endpoint_config_name"],
    )

    out = {"updated_endpoint": ENDPOINT_NAME, **names}
    logger.info(str(out))
    return out
```

Create `lambdas/update_endpoint/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [ ] **Step 4: Run tests + commit**

Run: `pytest lambdas/update_endpoint -v`
Expected: PASS (2 tests).

```bash
git add lambdas/update_endpoint/
git commit -m "Phase 7c: update_endpoint Lambda (refresh SageMaker endpoint to new model)"
```

---

## Task 5: Extend `query_api` with `/routes/{routeId}/prediction`

**Files:**
- Modify: `lambdas/query_api/handler.py`
- Modify: `lambdas/query_api/tests/test_handler.py`

- [ ] **Step 1: Append failing tests**

Add to `lambdas/query_api/tests/test_handler.py`:
```python
def test_prediction_route_returns_row_when_present(monkeypatch):
    from lambdas.query_api import handler as qa
    table = MagicMock()
    table.get_item.return_value = {"Item": {
        "route_id": "720",
        "predicted_next_window_avg_delay_seconds": Decimal("132"),
        "current_avg_delay_seconds": Decimal("75"),
        "model_version": "v=2026-06-15",
        "window_start_iso": "2026-05-27T12:05:00Z",
        "as_of": "2026-05-27T12:06:30Z",
    }}
    monkeypatch.setattr(qa, "_predictions", lambda: table)
    event = {
        "resource": "/routes/{routeId}/prediction",
        "httpMethod": "GET",
        "pathParameters": {"routeId": "720"},
    }
    resp = qa.lambda_handler(event, MagicMock())
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["route_id"] == "720"
    assert body["predicted_next_window_avg_delay_seconds"] == 132
    assert body["current_avg_delay_seconds"] == 75


def test_prediction_route_returns_404_when_missing(monkeypatch):
    from lambdas.query_api import handler as qa
    table = MagicMock()
    table.get_item.return_value = {}
    monkeypatch.setattr(qa, "_predictions", lambda: table)
    event = {
        "resource": "/routes/{routeId}/prediction",
        "httpMethod": "GET",
        "pathParameters": {"routeId": "720"},
    }
    resp = qa.lambda_handler(event, MagicMock())
    assert resp["statusCode"] == 404
```

(Note: this test imports `json`, `Decimal`, and `MagicMock`. The existing `lambdas/query_api/tests/test_handler.py` should already have these; if not, add `from decimal import Decimal` and `from unittest.mock import MagicMock` to the test file's imports.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/query_api -v`
Expected: FAIL on the two new tests.

- [ ] **Step 3: Implement**

In `lambdas/query_api/handler.py`, add the env var + lazy table near the other globals:
```python
ROUTE_PREDICTIONS_TABLE_NAME = os.environ.get("ROUTE_PREDICTIONS_TABLE_NAME", "")

_predictions_table = None


def _predictions():
    global _dynamodb, _predictions_table
    if _predictions_table is None:
        if not ROUTE_PREDICTIONS_TABLE_NAME:
            raise RuntimeError("ROUTE_PREDICTIONS_TABLE_NAME env var not set")
        _dynamodb = _dynamodb or boto3.resource("dynamodb")
        _predictions_table = _dynamodb.Table(ROUTE_PREDICTIONS_TABLE_NAME)
    return _predictions_table
```

Add the handler function (next to `handle_route_aggregates`):
```python
def handle_route_prediction(event: dict[str, Any]) -> dict:
    path_params = event.get("pathParameters") or {}
    route_id = path_params.get("routeId") or ""
    if not route_id:
        return _response(400, {"error": "missing_route_id"})
    resp = _predictions().get_item(Key={"route_id": route_id})
    item = resp.get("Item")
    if not item:
        return _response(404, {"error": "no_prediction", "route_id": route_id})
    return _response(200, {
        "route_id": item.get("route_id"),
        "predicted_next_window_avg_delay_seconds":
            _decimal_to_number(item.get("predicted_next_window_avg_delay_seconds")),
        "current_avg_delay_seconds":
            _decimal_to_number(item.get("current_avg_delay_seconds")),
        "model_version": item.get("model_version"),
        "window_start_iso": item.get("window_start_iso"),
        "as_of": item.get("as_of"),
    })
```

Wire it into the dispatch in `lambda_handler`:
```python
    if resource == "/routes/{routeId}/prediction":
        return handle_route_prediction(event)
```
(Place it next to the existing `/routes/{routeId}/aggregates` dispatch.)

- [ ] **Step 4: Run tests + commit**

Run: `pytest lambdas/query_api -v`
Expected: previous 22 + 2 new = 24 PASS.

```bash
git add lambdas/query_api/
git commit -m "Phase 7c: query-api — /routes/{routeId}/prediction (reads route-predictions DDB)"
```

---

## Task 6: ApiStack — add `/routes/{routeId}/prediction` resource

**Files:**
- Modify: `cdk/lib/api-stack.ts`

- [ ] **Step 1: Extend props + grant + add route**

Add a required prop to `ApiStackProps`:
```typescript
  routePredictionsTable: dynamodb.ITable;
```

In the existing query-api Lambda's `environment` block, add:
```typescript
        ROUTE_PREDICTIONS_TABLE_NAME: props.routePredictionsTable.tableName,
```

After the existing `props.routeAggregatesTable.grantReadData(queryFn);` line, add:
```typescript
    props.routePredictionsTable.grantReadData(queryFn);
```

In the `/routes/{routeId}` resource block (next to the `aggregates` subresource), add:
```typescript
    const prediction = routeById.addResource('prediction');
    prediction.addMethod('GET');   // public, uses the default queryFn handler
```

- [ ] **Step 2: Type-check**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cdk/lib/api-stack.ts
git commit -m "Phase 7c: ApiStack — public /routes/{routeId}/prediction resource"
```

---

## Task 7: MLStack — SageMaker endpoint + precompute Lambda + state-machine wiring + cdk.ts wire-through

**Files:**
- Modify: `cdk/lib/ml-stack.ts`
- Modify: `cdk/bin/cdk.ts`

- [ ] **Step 1: Add props + the precompute Lambda + the endpoint + the update-endpoint Lambda**

In `cdk/lib/ml-stack.ts`, extend `MLStackProps`:
```typescript
  routePredictionsTable: dynamodb.ITable;
```

After the existing 7b Lambdas in the constructor, add the precompute Lambda:
```typescript
    // ---- precompute-predictions Lambda + 5-min schedule ----
    const preName = 'la-metro-precompute-predictions';
    const preLog = new logs.LogGroup(this, 'PrecomputeFnLogs', {
      logGroupName: `/aws/lambda/${preName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const endpointName = 'la-metro-delay-predictor';
    const precomputeFn = new lambda.Function(this, 'PrecomputeFn', {
      functionName: preName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'precompute_predictions', '.build'),
      ),
      memorySize: 512,
      // 60s: scan + 150 endpoint invokes + 150 DDB puts. Plenty of cushion.
      timeout: cdk.Duration.seconds(60),
      environment: {
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
        ROUTE_AGGREGATES_ROUTE_GSI: 'route_id-last_updated-index',
        ROUTE_PREDICTIONS_TABLE_NAME: props.routePredictionsTable.tableName,
        WEATHER_CACHE_TABLE_NAME: props.weatherCacheTable.tableName,
        MODELS_PREFIX_URI: `s3://${props.archiveBucket.bucketName}/models`,
        SAGEMAKER_ENDPOINT_NAME: endpointName,
        PREDICTION_TTL_SECONDS: '900',
      },
      logGroup: preLog,
      description: 'Phase 7c: per-route prediction precompute (5 min).',
    });
    props.routeAggregatesTable.grantReadData(precomputeFn);
    // route_id GSI read access (mirror the existing pattern in api-stack).
    precomputeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['dynamodb:Query', 'dynamodb:Scan'],
      resources: [`${props.routeAggregatesTable.tableArn}/index/route_id-last_updated-index`],
    }));
    props.routePredictionsTable.grantWriteData(precomputeFn);
    props.weatherCacheTable.grantReadData(precomputeFn);
    props.archiveBucket.grantRead(precomputeFn, 'models/*');
    precomputeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sagemaker:InvokeEndpoint'],
      resources: [
        `arn:aws:sagemaker:${this.region}:${this.account}:endpoint/${endpointName}`,
      ],
    }));

    new events.Rule(this, 'PrecomputeSchedule', {
      ruleName: 'la-metro-precompute-predictions-schedule',
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      targets: [new targets.LambdaFunction(precomputeFn)],
      description: 'Phase 7c: triggers precompute every 5 min.',
    });
```

Add the SageMaker execution role + Model + EndpointConfig + Endpoint:
```typescript
    // ---- SageMaker Serverless endpoint ----
    const sagemakerExecRole = new iam.Role(this, 'SageMakerExecutionRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      description: 'Phase 7c: SageMaker endpoint execution role (reads models/*).',
    });
    props.archiveBucket.grantRead(sagemakerExecRole, 'models/*');
    sagemakerExecRole.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: ['*'],
    }));

    const xgboostImage =
      '746614075791.dkr.ecr.us-west-2.amazonaws.com/sagemaker-xgboost:1.7-1';

    // The Model points at models/current/model.tar.gz which must already
    // exist (produced by 7b's first successful pipeline run). If you deploy
    // 7c before 7b has run, CreateModel will fail with NoSuchKey.
    const initialModel = new cdk.aws_sagemaker.CfnModel(this, 'InitialModel', {
      modelName: 'la-metro-delay-predictor-initial',
      executionRoleArn: sagemakerExecRole.roleArn,
      primaryContainer: {
        image: xgboostImage,
        modelDataUrl: `s3://${props.archiveBucket.bucketName}/models/current/model.tar.gz`,
      },
    });

    const initialEndpointConfig = new cdk.aws_sagemaker.CfnEndpointConfig(
      this, 'InitialEndpointConfig', {
        endpointConfigName: 'la-metro-delay-predictor-cfg-initial',
        productionVariants: [{
          variantName: 'AllTraffic',
          modelName: initialModel.attrModelName,
          serverlessConfig: { memorySizeInMb: 1024, maxConcurrency: 5 },
        }],
      },
    );
    initialEndpointConfig.addDependency(initialModel);

    const endpoint = new cdk.aws_sagemaker.CfnEndpoint(this, 'Endpoint', {
      endpointName,
      endpointConfigName: initialEndpointConfig.attrEndpointConfigName,
    });
    endpoint.addDependency(initialEndpointConfig);

    new cdk.CfnOutput(this, 'SagemakerEndpointName', { value: endpointName });

    // ---- update_endpoint Lambda (called from Step Functions after Promote) ----
    const upName = 'la-metro-update-endpoint';
    const upLog = new logs.LogGroup(this, 'UpdateEndpointFnLogs', {
      logGroupName: `/aws/lambda/${upName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const updateEndpointFn = new lambda.Function(this, 'UpdateEndpointFn', {
      functionName: upName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'update_endpoint', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      environment: {
        SAGEMAKER_ENDPOINT_NAME: endpointName,
        TRAINING_IMAGE: xgboostImage,
        SAGEMAKER_EXECUTION_ROLE_ARN: sagemakerExecRole.roleArn,
        ENDPOINT_MEMORY_MB: '1024',
        ENDPOINT_MAX_CONCURRENCY: '5',
      },
      logGroup: upLog,
      description: 'Phase 7c: refresh SageMaker endpoint to a freshly-promoted model.',
    });
    updateEndpointFn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'sagemaker:CreateModel', 'sagemaker:CreateEndpointConfig',
        'sagemaker:UpdateEndpoint',
      ],
      resources: ['*'],
    }));
    updateEndpointFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['iam:PassRole'],
      resources: [sagemakerExecRole.roleArn],
    }));
```

- [ ] **Step 2: Extend the Step Functions state machine to call `update_endpoint` after `Promote`**

In the JSON `definition` object built in 7b's Task 5, change the `Promote` state's `End: true` to `Next: 'UpdateEndpoint'` and add a new state after it:
```typescript
        Promote: {
          // (unchanged shape, except for this swap:)
          End: false,
          Next: 'UpdateEndpoint',
        },
        UpdateEndpoint: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: updateEndpointFn.functionArn,
            Payload: {
              'promoted_version.$': '$.promote.result.promoted_version',
              'current_model_uri.$': '$.promote.result.current_model_uri',
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.endpoint',
          End: true,
        },
```
Also, in the Step Functions role's `lambda:InvokeFunction` policy resources list (also defined in 7b's Task 5), append `updateEndpointFn.functionArn`:
```typescript
              resources: [sufficiencyFn.functionArn, evaluateFn.functionArn, promoteFn.functionArn, updateEndpointFn.functionArn],
```

> If the Step Functions definition was committed as a separate file in 7b's execution rather than inline as planned, edit it in place.

- [ ] **Step 3: Wire `routePredictionsTable` through `cdk/bin/cdk.ts`**

Add to the `MLStack` props object:
```typescript
  routePredictionsTable: storage.routePredictionsTable,
```
Add to the `ApiStack` props object:
```typescript
  routePredictionsTable: storage.routePredictionsTable,
```

- [ ] **Step 4: Build everything + full synth**

Run:
```bash
cd /Users/caden/awsProject
for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot data_sufficiency_check evaluate_model promote_model precompute_predictions update_endpoint; do
  scripts/build-lambda.sh "$d"
done
cd cdk && npx tsc --noEmit && npx cdk synth --quiet
```
Expected: full app synth succeeds (all 9 stacks); no missing-asset errors.

- [ ] **Step 5: Commit**

```bash
git add cdk/lib/ml-stack.ts cdk/bin/cdk.ts
git commit -m "Phase 7c: SageMaker endpoint + precompute + update_endpoint + state-machine wiring"
```

---

## Task 8: Frontend — `fetchRoutePrediction` + trendline card

**Files:**
- Modify: `frontend/lib/api.ts`
- Modify: `frontend/app/route/page.tsx`

- [ ] **Step 1: Add the client function + types in `frontend/lib/api.ts`**

Append to the file (next to `fetchRouteAggregates`):
```typescript
export interface RoutePrediction {
  route_id: string;
  predicted_next_window_avg_delay_seconds: number;
  current_avg_delay_seconds: number;
  model_version: string;
  window_start_iso: string;
  as_of: string;
}

export async function fetchRoutePrediction(
  routeId: string,
  signal?: AbortSignal,
): Promise<RoutePrediction | null> {
  const base = apiBase();
  if (!base) return null;
  const url = `${base}/routes/${encodeURIComponent(routeId)}/prediction`;
  const res = await fetch(url, { signal, cache: 'no-store' });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`API ${res.status}`);
  return (await res.json()) as RoutePrediction;
}
```

- [ ] **Step 2: Add the trendline card in `frontend/app/route/page.tsx`**

In the route page's component, alongside the existing aggregate state, add:
```tsx
import { fetchRoutePrediction, type RoutePrediction } from '@/lib/api';
// ... existing imports ...

// inside RoutePageInner:
const [prediction, setPrediction] = useState<RoutePrediction | null>(null);

useEffect(() => {
  if (!routeId) return;
  const ctrl = new AbortController();
  fetchRoutePrediction(routeId, ctrl.signal)
    .then(setPrediction)
    .catch(() => setPrediction(null));
  const id = setInterval(() => {
    fetchRoutePrediction(routeId).then(setPrediction).catch(() => {});
  }, 60_000);
  return () => { ctrl.abort(); clearInterval(id); };
}, [routeId]);
```

Add the card in the JSX (insert as a new `<section>` immediately after the existing 3-stat headline grid, before the charts):
```tsx
{prediction && (
  <section className="mt-6 rounded bg-zinc-900/50 p-4">
    <div className="text-xs uppercase tracking-wide text-zinc-500">trendline</div>
    <div className="mt-1 text-lg">
      Currently <span className="font-mono">
        {formatDelay(prediction.current_avg_delay_seconds)}
      </span>, predicted <span className="font-mono">
        {formatDelay(prediction.predicted_next_window_avg_delay_seconds)}
      </span>{' '}
      <TrendArrow
        current={prediction.current_avg_delay_seconds}
        predicted={prediction.predicted_next_window_avg_delay_seconds}
      />
    </div>
    <div className="mt-1 text-xs text-zinc-500">
      model {prediction.model_version}
    </div>
  </section>
)}
```

Add these helper components/functions at the bottom of the file (alongside `Stat`):
```tsx
function formatDelay(seconds: number): string {
  const sign = seconds > 0 ? '+' : seconds < 0 ? '−' : '';
  const mins = Math.round(Math.abs(seconds) / 60);
  return `${sign}${mins} min`;
}

function TrendArrow({ current, predicted }: { current: number; predicted: number }) {
  const delta = predicted - current;
  if (Math.abs(delta) < 30) return <span aria-label="steady">→</span>;  // <30s = flat
  const up = delta > 0;
  return (
    <span
      className={up ? 'text-red-400' : 'text-emerald-400'}
      aria-label={up ? 'worsening' : 'improving'}
    >
      {up ? '↑' : '↓'}
    </span>
  );
}
```

- [ ] **Step 3: Build + commit**

Run from `frontend/`: `npm run build`
Expected: builds clean.

```bash
git add frontend/lib/api.ts frontend/app/route/page.tsx
git commit -m "Phase 7c: frontend trendline card on route detail page"
```

---

## Task 9: CI build loop addition

**Files:**
- Modify: `.github/workflows/pr-checks.yml`

- [ ] **Step 1: Extend the loop**

```yaml
          for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot data_sufficiency_check evaluate_model promote_model precompute_predictions update_endpoint; do
            scripts/build-lambda.sh "$d"
          done
```

- [ ] **Step 2: Full Python suite**

Run: `pytest -q`
Expected: previous 169 + (7 + 3 + 2 + 2 = 14) new = **183 passed**.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pr-checks.yml
git commit -m "Phase 7c: add precompute_predictions + update_endpoint to CI build list"
```

---

## Deployment & manual verification (after all tasks)

**Order matters** — the SageMaker endpoint will fail to create if no model exists at `models/current/model.tar.gz`.

- [ ] Run 7b's pipeline at least once (manually via the Step Functions console) so `models/current/model.tar.gz` exists.
- [ ] `cd cdk && npx cdk deploy LaMetro-StorageStack LaMetro-MLStack LaMetro-ApiStack` (Storage adds the route-predictions table; MLStack creates the endpoint + precompute + update-endpoint; ApiStack adds the prediction route).
- [ ] Wait ~5 min, then check CloudWatch logs `/aws/lambda/la-metro-precompute-predictions` — expect log lines like `{"routes_attempted": N, "predictions_written": N, "model_version": "v=..."}`.
- [ ] `curl https://<api-id>.execute-api.us-west-2.amazonaws.com/prod/routes/720/prediction` — expect JSON with `predicted_next_window_avg_delay_seconds`, `current_avg_delay_seconds`, etc.
- [ ] Open the route detail page in the frontend — confirm the trendline card renders.
- [ ] (Optional) Trigger Step Functions; confirm after `Promote` the `UpdateEndpoint` state runs and the endpoint gets a new EndpointConfig.

---

## Self-review notes (author)

- **Spec coverage:** 7c maps to Task 1 (route-predictions), Tasks 2–3 (precompute Lambda), Task 4 (update_endpoint Lambda), Task 5 (query-api prediction route), Task 6 (ApiStack), Task 7 (MLStack endpoint + precompute schedule + state-machine extension + cdk.ts wiring), Task 8 (frontend), Task 9 (CI).
- **Type consistency:** `route_id` PK + `predicted_next_window_avg_delay_seconds` + `current_avg_delay_seconds` + `model_version` + `window_start_iso` + `as_of` + `ttl_epoch` shape consistent between the precompute writer, the route-predictions table, the query-api reader, the frontend TS type, and the trendline card.
- **Critical assumption flagged:** `derive_route_code()` Python hash MUST match the Athena route-code derivation in 7b's SQL — verify after 7b's first real run that the codes line up (predictions on a route with no historical training data would still be valid as the model generalizes via the other features, but the per-route-code signal is lost if they don't match). Reconfirm before deploying 7c.
- **First-deploy ordering hazard:** the `CfnModel` resource fails if `models/current/model.tar.gz` is absent. Deploy order documented in the verification block.
