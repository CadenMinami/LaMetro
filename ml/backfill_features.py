"""Backfill route_window_features from raw GTFS-RT events in S3.

One-time local script. See docs/superpowers/specs/2026-06-02-feature-backfill-design.md.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator
from zoneinfo import ZoneInfo

LA_TZ = ZoneInfo("America/Los_Angeles")
WINDOW_MINUTES = 5

_DECODER = json.JSONDecoder()


def iter_json_objects(raw: bytes) -> Iterator[dict[str, Any]]:
    """Yield each JSON object from a Firehose blob of *concatenated* JSON
    (no delimiter between objects). Stops at the first undecodable tail."""
    text = raw.decode("utf-8")
    i, n = 0, len(text)
    while i < n:
        # Skip inter-object whitespace/newlines.
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            obj, end = _DECODER.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end


def is_routed(rec: dict[str, Any]) -> bool:
    """True only when the record can be schedule-matched (has route + trip)."""
    return bool(rec.get("route_id")) and bool(rec.get("trip_id"))


def seconds_into_service_day(epoch: int) -> int:
    """Seconds since LA-local midnight for a unix timestamp. Used as the
    service-day clock the GTFS schedule is expressed in. (Owl trips that cross
    midnight fall outside the schedule window and yield a null delay — an
    accepted edge for aggregate features.)"""
    local = datetime.fromtimestamp(int(epoch), tz=LA_TZ)
    return local.hour * 3600 + local.minute * 60 + local.second
