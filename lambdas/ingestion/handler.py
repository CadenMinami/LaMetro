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

import boto3
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
SECRET_NAME = os.environ.get("SWIFTLY_SECRET_NAME", "")
# Local-dev escape hatch: skip Secrets Manager and read the key directly.
LOCAL_API_KEY = os.environ.get("LA_METRO_API_KEY", "")
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "8"))

_secrets_client = None
_cached_api_key: str | None = None


def get_api_key() -> str:
    """Resolve the Swiftly key, preferring local env var, else Secrets Manager.

    Cached at module scope: cold start pays the SM call, warm invocations don't.
    """
    global _secrets_client, _cached_api_key
    if LOCAL_API_KEY:
        return LOCAL_API_KEY
    if _cached_api_key is not None:
        return _cached_api_key
    if not SECRET_NAME:
        raise RuntimeError("SWIFTLY_SECRET_NAME not set and no LA_METRO_API_KEY fallback")
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    resp = _secrets_client.get_secret_value(SecretId=SECRET_NAME)
    _cached_api_key = resp["SecretString"]
    return _cached_api_key


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
        payload = fetch_feed(FEED_URL, HTTP_TIMEOUT_SECONDS, get_api_key())
        feed = parse_feed(payload)
        summary = summarize(feed)
    except Exception as exc:
        logger.exception("ingestion_failed")
        return {"ok": False, "error": str(exc)}

    elapsed_ms = int((time.monotonic() - start) * 1000)
    log_record = {"ok": True, "elapsed_ms": elapsed_ms, **summary}
    logger.info(json.dumps(log_record, default=str))
    return log_record
