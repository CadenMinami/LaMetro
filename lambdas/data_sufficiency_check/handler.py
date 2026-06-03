"""Data-sufficiency check — Phase 7b.

Inspects the Athena UNLOAD manifest and decides whether the training set is
large enough to be worth training on. The Step Functions state machine
branches on the returned `sufficient` flag.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from typing import Any
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_THRESHOLD_ROWS = int(os.environ.get("DEFAULT_THRESHOLD_ROWS", "1000"))

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def row_count_from_manifest(raw: bytes) -> int | None:
    """Sum the `rows` column from an UNLOAD manifest CSV. Returns None if the
    manifest doesn't carry row counts (caller should use a fallback)."""
    reader = csv.reader(io.StringIO(raw.decode("utf-8")))
    rows = list(reader)
    if not rows:
        return 0
    header = rows[0]
    if "rows" not in header:
        return None
    rows_idx = header.index("rows")
    total = 0
    for r in rows[1:]:
        if not r:
            continue
        try:
            total += int(r[rows_idx])
        except (IndexError, ValueError):
            continue
    return total


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    manifest_uri = event["manifest_uri"]
    threshold = int(event.get("threshold_rows", DEFAULT_THRESHOLD_ROWS))

    bucket, key = _split_s3(manifest_uri)
    raw = _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    rows = row_count_from_manifest(raw)
    if rows is None:
        # Fallback path: read each listed file via head_object isn't sufficient,
        # so for v1 we treat unknown as sufficient=False (safer than running
        # training on possibly tiny data).
        logger.warning("manifest_lacks_row_count; defaulting to insufficient")
        rows = 0

    result = {
        "sufficient": rows >= threshold,
        "row_count": rows,
        "threshold_rows": threshold,
    }
    logger.info(str(result))
    return result
