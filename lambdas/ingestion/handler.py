"""Ingest LA Metro GTFS-RT vehicle positions feed.

Phase 2: fetch the feed, parse the protobuf, emit one Kinesis record per
active vehicle. Downstream consumers (enrichment Lambda, Firehose) fan out
from there.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any, Iterable

import boto3
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FEED_URL = os.environ.get(
    "LA_METRO_FEED_URL",
    "https://api.goswift.ly/real-time/lametro/gtfs-rt-vehicle-positions",
)
SECRET_NAME = os.environ.get("SWIFTLY_SECRET_NAME", "")
LOCAL_API_KEY = os.environ.get("LA_METRO_API_KEY", "")
STREAM_NAME = os.environ.get("VEHICLE_STREAM_NAME", "")
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "8"))

# Kinesis PutRecords hard limit: 500 records per call.
KINESIS_BATCH_SIZE = 500

_secrets_client = None
_kinesis_client = None
_cached_api_key: str | None = None


def get_api_key() -> str:
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


def get_kinesis_client():
    global _kinesis_client
    if _kinesis_client is None:
        _kinesis_client = boto3.client("kinesis")
    return _kinesis_client


def fetch_feed(url: str, timeout: float, api_key: str = "") -> bytes:
    headers = {"User-Agent": "la-metro-reliability/0.2"}
    if api_key:
        headers["Authorization"] = api_key
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def vehicle_events(feed: gtfs_realtime_pb2.FeedMessage) -> Iterable[dict[str, Any]]:
    """Yield one dict per vehicle entity in the feed.

    Skips entities without position data or with empty vehicle_id (those can't
    be deduplicated downstream).
    """
    feed_ts = feed.header.timestamp
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        vehicle_id = v.vehicle.id
        if not vehicle_id:
            continue
        if not v.HasField("position"):
            continue
        yield {
            "vehicle_id": vehicle_id,
            "route_id": v.trip.route_id,
            "trip_id": v.trip.trip_id,
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "bearing": v.position.bearing if v.position.HasField("bearing") else None,
            "speed_mps": v.position.speed if v.position.HasField("speed") else None,
            "vehicle_timestamp": v.timestamp,
            "feed_timestamp": feed_ts,
        }


def put_records_batched(stream_name: str, events: list[dict[str, Any]]) -> dict[str, int]:
    """PutRecords in chunks of 500. Returns counts of sent vs failed records."""
    client = get_kinesis_client()
    sent = 0
    failed = 0
    for i in range(0, len(events), KINESIS_BATCH_SIZE):
        chunk = events[i : i + KINESIS_BATCH_SIZE]
        records = [
            {
                "Data": json.dumps(e).encode("utf-8"),
                # Partition by vehicle_id so updates for the same vehicle land on
                # the same shard and stay ordered.
                "PartitionKey": e["vehicle_id"],
            }
            for e in chunk
        ]
        resp = client.put_records(StreamName=stream_name, Records=records)
        chunk_failed = resp.get("FailedRecordCount", 0)
        sent += len(chunk) - chunk_failed
        failed += chunk_failed
    return {"sent": sent, "failed": failed}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    start = time.monotonic()
    try:
        payload = fetch_feed(FEED_URL, HTTP_TIMEOUT_SECONDS, get_api_key())
        feed = parse_feed(payload)
        events = list(vehicle_events(feed))
        if not STREAM_NAME:
            raise RuntimeError("VEHICLE_STREAM_NAME env var not set")
        result = put_records_batched(STREAM_NAME, events) if events else {"sent": 0, "failed": 0}
    except Exception as exc:
        logger.exception("ingestion_failed")
        return {"ok": False, "error": str(exc)}

    elapsed_ms = int((time.monotonic() - start) * 1000)
    log_record = {
        "ok": True,
        "elapsed_ms": elapsed_ms,
        "vehicle_count": len(events),
        "kinesis_sent": result["sent"],
        "kinesis_failed": result["failed"],
        "feed_timestamp": feed.header.timestamp,
    }
    logger.info(json.dumps(log_record, default=str))
    return log_record
