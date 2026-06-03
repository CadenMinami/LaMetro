"""Promote model — Phase 7b.

Called only when evaluate_model said `promote: true`. Copies the candidate
model artifact to two locations:
  - s3://<archive>/models/v=YYYY-MM-DD/model.tar.gz  (versioned trail)
  - s3://<archive>/models/current/model.tar.gz       (the live pointer)

Writes a fresh s3://<archive>/models/current/metrics.json that records what's
deployed. 7c's gate-and-promote will eventually extend this to also call
SageMaker UpdateEndpoint; for 7b we only update the registry in S3.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def version_key(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("v=%Y-%m-%d")


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    candidate_uri = event["candidate_model_uri"]
    models_prefix_uri = event["models_prefix_uri"].rstrip("/")
    candidate_metric = float(event["candidate_metric"])
    metric_name = event.get("metric_name", "validation:rmse")

    src_bucket, src_key = _split_s3(candidate_uri)
    dst_bucket, models_prefix = _split_s3(models_prefix_uri)
    now = _utcnow()
    vkey = version_key(now)

    versioned_key = f"{models_prefix}/{vkey}/model.tar.gz"
    current_key = f"{models_prefix}/current/model.tar.gz"
    metrics_key = f"{models_prefix}/current/metrics.json"

    s3 = _s3()
    copy_source = {"Bucket": src_bucket, "Key": src_key}
    s3.copy_object(Bucket=dst_bucket, Key=versioned_key, CopySource=copy_source)
    s3.copy_object(Bucket=dst_bucket, Key=current_key, CopySource=copy_source)

    metrics_body = {
        "validation_metric": candidate_metric,
        "metric_name": metric_name,
        "promoted_version": vkey,
        "promoted_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    s3.put_object(
        Bucket=dst_bucket,
        Key=metrics_key,
        Body=json.dumps(metrics_body).encode("utf-8"),
        ContentType="application/json",
    )

    result = {
        "promoted_version": vkey,
        "promoted_model_uri": f"s3://{dst_bucket}/{versioned_key}",
        "current_model_uri": f"s3://{dst_bucket}/{current_key}",
        "metrics_uri": f"s3://{dst_bucket}/{metrics_key}",
    }
    logger.info(str(result))
    return result
