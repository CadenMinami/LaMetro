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
