"""Precompute-predictions Lambda — Phase 7c.

Runs every 5 min on EventBridge. For each route with recent aggregate data,
assembles the live feature vector, calls the SageMaker Serverless endpoint,
and writes the prediction to the route-predictions DynamoDB table. The
public query API reads from that table, so user requests are instant.
"""

from __future__ import annotations

import json
import logging
import os
import zlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def derive_route_code(route_id: str) -> int:
    """Stable bucket in [0, 1000) — MUST match 7b's Athena route_code.

    7b's feature_extraction.sql uses `abs(crc32(to_utf8(route_id))) % 1000`.
    Python's `zlib.crc32` is the same standard CRC-32 (unsigned in Py3, so
    abs() is a no-op), so this reproduces the training-time route_code exactly.
    """
    return zlib.crc32(route_id.encode("utf-8")) % 1000


def _to_int_or_none(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def pick_recent_lags(rows: list[dict[str, Any]]) -> list[int]:
    """Return the 3 most recent avg_delay_seconds (newest first). Rows are
    expected pre-sorted newest first. Missing/null values are skipped; if fewer
    than 3 valid lags exist, the result is padded with 0 (matching the COALESCE
    behavior at training)."""
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
    """Single CSV line in the column order the trained XGBoost expects:
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


ROUTE_AGGREGATES_TABLE = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
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
    the recent past. v1: scan-with-projection on the BASE table for distinct
    route_ids. For our ~150-route scale this is acceptably cheap; if it ever
    becomes hot, switch to a maintained 'active_routes' record."""
    table = _route_aggregates()
    seen: set[str] = set()
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
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
    """Query the route-aggregates BASE table for the most recent windows of one
    route. PK=route_id, SK=window_start_iso, so ScanIndexForward=False returns
    newest first. (route-aggregates has NO route_id GSI — PK is already route_id.)"""
    table = _route_aggregates()
    resp = table.query(
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
