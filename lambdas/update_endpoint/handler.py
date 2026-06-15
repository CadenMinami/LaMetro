"""Update SageMaker endpoint — Phase 7c.

Called by the Step Functions pipeline immediately after promote_model. Creates
a new SageMaker Model (timestamped by promoted_version) + EndpointConfig and
calls UpdateEndpoint so the live endpoint serves the freshly-promoted model.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_NAME = os.environ.get("SAGEMAKER_ENDPOINT_NAME", "")
TRAINING_IMAGE = os.environ.get("TRAINING_IMAGE", "")
EXECUTION_ROLE_ARN = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN", "")
MEMORY_SIZE_MB = os.environ.get("ENDPOINT_MEMORY_MB", "1024")
MAX_CONCURRENCY = os.environ.get("ENDPOINT_MAX_CONCURRENCY", "5")

_sm = None


def _sagemaker():
    global _sm
    if _sm is None:
        _sm = boto3.client("sagemaker")
    return _sm


def _sanitize(name: str) -> str:
    """SageMaker resource names allow [a-zA-Z0-9-]; replace = and . in a
    version like `v=2026-06-15`."""
    return name.replace("=", "-").replace(".", "-").replace("_", "-")


def resource_names(promoted_version: str) -> dict[str, str]:
    v = _sanitize(promoted_version)
    return {
        "model_name": f"la-metro-delay-predictor-{v}",
        "endpoint_config_name": f"la-metro-delay-predictor-cfg-{v}",
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not (ENDPOINT_NAME and TRAINING_IMAGE and EXECUTION_ROLE_ARN):
        raise RuntimeError(
            "Missing env: SAGEMAKER_ENDPOINT_NAME / TRAINING_IMAGE / "
            "SAGEMAKER_EXECUTION_ROLE_ARN"
        )

    promoted_version = event["promoted_version"]
    model_uri = event["current_model_uri"]
    names = resource_names(promoted_version)

    sm = _sagemaker()
    sm.create_model(
        ModelName=names["model_name"],
        # No SAGEMAKER_PROGRAM: the built-in XGBoost 1.7-1 container loads the
        # pickled `xgboost-model` from model.tar.gz via its default model_fn —
        # same as the InitialModel. A custom-script env would break that.
        PrimaryContainer={
            "Image": TRAINING_IMAGE,
            "ModelDataUrl": model_uri,
        },
        ExecutionRoleArn=EXECUTION_ROLE_ARN,
    )
    sm.create_endpoint_config(
        EndpointConfigName=names["endpoint_config_name"],
        ProductionVariants=[{
            "VariantName": "AllTraffic",
            "ModelName": names["model_name"],
            "ServerlessConfig": {
                "MemorySizeInMB": int(MEMORY_SIZE_MB),
                "MaxConcurrency": int(MAX_CONCURRENCY),
            },
        }],
    )
    sm.update_endpoint(
        EndpointName=ENDPOINT_NAME,
        EndpointConfigName=names["endpoint_config_name"],
    )

    out = {"updated_endpoint": ENDPOINT_NAME, **names}
    logger.info(str(out))
    return out
