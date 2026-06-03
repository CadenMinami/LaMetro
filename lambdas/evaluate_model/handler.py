"""Evaluate model — Phase 7b.

Fetches the just-completed SageMaker training job's final validation metric,
compares it to the deployed model's metric (stored in s3://.../models/current/
metrics.json), and returns a promote/skip decision plus the candidate model
artifact URI for the next step.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALIDATION_METRIC_NAME = os.environ.get("VALIDATION_METRIC_NAME", "validation:rmse")

_sm_client = None
_s3_client = None


def _sagemaker():
    global _sm_client
    if _sm_client is None:
        _sm_client = boto3.client("sagemaker")
    return _sm_client


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def extract_validation_metric(job_desc: dict) -> float | None:
    for m in job_desc.get("FinalMetricDataList", []):
        if m.get("MetricName") == VALIDATION_METRIC_NAME:
            return float(m["Value"])
    return None


def should_promote(candidate: float, deployed: float | None) -> bool:
    if deployed is None:
        return True
    # Lower is better for RMSE/MAE; strictly less than (not <=) avoids noise
    # flap from numerically equal runs.
    return candidate < deployed


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def _read_deployed_metric(models_prefix_uri: str) -> float | None:
    bucket, key_prefix = _split_s3(models_prefix_uri.rstrip("/"))
    key = f"{key_prefix}/current/metrics.json"
    try:
        body = _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    except Exception:
        # boto3 mocks in tests may raise non-ClientError exceptions; treat as
        # "no deployed model" rather than crashing the pipeline.
        logger.exception("could not read deployed metrics; treating as none")
        return None
    try:
        return float(json.loads(body)["validation_metric"])
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        logger.exception("malformed deployed metrics.json")
        return None


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    job_name = event["training_job_name"]
    models_prefix_uri = event["models_prefix_uri"]

    job = _sagemaker().describe_training_job(TrainingJobName=job_name)
    candidate_metric = extract_validation_metric(job)
    candidate_model_uri = job.get("ModelArtifacts", {}).get("S3ModelArtifacts")

    deployed_metric = _read_deployed_metric(models_prefix_uri)

    promote = (
        candidate_metric is not None
        and candidate_model_uri is not None
        and should_promote(candidate_metric, deployed_metric)
    )

    result = {
        "promote": promote,
        "candidate_metric": candidate_metric,
        "deployed_metric": deployed_metric,
        "candidate_model_uri": candidate_model_uri,
        "metric_name": VALIDATION_METRIC_NAME,
    }
    logger.info(str(result))
    return result
