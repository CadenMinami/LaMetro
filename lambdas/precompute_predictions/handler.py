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
