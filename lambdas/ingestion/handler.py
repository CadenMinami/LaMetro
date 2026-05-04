"""Ingest LA Metro GTFS-RT vehicle positions feed.

Phase 1: fetch, parse, log a structured summary. No downstream emit yet —
Kinesis comes in Phase 2.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any

from google.transit import gtfs_realtime_pb2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Default points at LA Metro's Swiftly endpoint (requires a Swiftly API key).
# Override with LA_METRO_FEED_URL env var for development against another feed
# (e.g., MTA NYC, which is public and needs no key).
FEED_URL = os.environ.get(
    "LA_METRO_FEED_URL",
    "https://api.goswift.ly/real-time/lametro/gtfs-rt-vehicle-positions",
)
API_KEY = os.environ.get("LA_METRO_API_KEY", "")
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "8"))


def fetch_feed(url: str, timeout: float, api_key: str = "") -> bytes:
    headers = {"User-Agent": "la-metro-reliability/0.1"}
    if api_key:
        headers["Authorization"] = api_key
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def summarize(feed: gtfs_realtime_pb2.FeedMessage) -> dict[str, Any]:
    vehicle_count = 0
    sample = None
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle_count += 1
        if sample is None:
            v = entity.vehicle
            sample = {
                "vehicle_id": v.vehicle.id,
                "route_id": v.trip.route_id,
                "lat": v.position.latitude,
                "lon": v.position.longitude,
                "timestamp": v.timestamp,
            }
    return {
        "vehicle_count": vehicle_count,
        "feed_timestamp": feed.header.timestamp,
        "sample": sample,
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    start = time.monotonic()
    try:
        payload = fetch_feed(FEED_URL, HTTP_TIMEOUT_SECONDS, API_KEY)
        feed = parse_feed(payload)
        summary = summarize(feed)
    except Exception as exc:
        logger.exception("ingestion_failed")
        return {"ok": False, "error": str(exc)}

    elapsed_ms = int((time.monotonic() - start) * 1000)
    log_record = {"ok": True, "elapsed_ms": elapsed_ms, **summary}
    logger.info(json.dumps(log_record, default=str))
    return log_record
