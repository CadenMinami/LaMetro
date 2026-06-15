"""Unit tests for the update-endpoint Lambda (Phase 7c)."""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.update_endpoint import handler


def test_resource_names_use_promoted_version():
    names = handler.resource_names("v=2026-06-15")
    assert names["model_name"] == "la-metro-delay-predictor-v-2026-06-15"
    assert names["endpoint_config_name"] == "la-metro-delay-predictor-cfg-v-2026-06-15"


def test_lambda_handler_creates_model_config_and_updates_endpoint(monkeypatch):
    sm = MagicMock()
    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "ENDPOINT_NAME", "la-metro-delay-predictor")
    monkeypatch.setattr(handler, "TRAINING_IMAGE", "img/xgboost:1.7-1")
    monkeypatch.setattr(handler, "EXECUTION_ROLE_ARN", "arn:aws:iam::123:role/SageMakerExec")
    monkeypatch.setattr(handler, "MEMORY_SIZE_MB", "1024")
    monkeypatch.setattr(handler, "MAX_CONCURRENCY", "5")

    event = {
        "promoted_version": "v=2026-06-15",
        "current_model_uri": "s3://bkt/models/current/model.tar.gz",
    }
    out = handler.lambda_handler(event, MagicMock())
    assert out["updated_endpoint"] == "la-metro-delay-predictor"

    sm.create_model.assert_called_once()
    cm = sm.create_model.call_args.kwargs
    assert cm["ModelName"] == "la-metro-delay-predictor-v-2026-06-15"
    assert cm["PrimaryContainer"]["Image"] == "img/xgboost:1.7-1"
    assert cm["PrimaryContainer"]["ModelDataUrl"] == "s3://bkt/models/current/model.tar.gz"
    assert cm["ExecutionRoleArn"] == "arn:aws:iam::123:role/SageMakerExec"

    sm.create_endpoint_config.assert_called_once()
    ec = sm.create_endpoint_config.call_args.kwargs
    assert ec["EndpointConfigName"] == "la-metro-delay-predictor-cfg-v-2026-06-15"
    variant = ec["ProductionVariants"][0]
    assert variant["ModelName"] == "la-metro-delay-predictor-v-2026-06-15"
    assert variant["ServerlessConfig"]["MemorySizeInMB"] == 1024
    assert variant["ServerlessConfig"]["MaxConcurrency"] == 5

    sm.update_endpoint.assert_called_once_with(
        EndpointName="la-metro-delay-predictor",
        EndpointConfigName="la-metro-delay-predictor-cfg-v-2026-06-15",
    )
