"""Unit tests for the evaluate-model Lambda (Phase 7b)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from lambdas.evaluate_model import handler


def test_extract_validation_metric_returns_value():
    job_desc = {
        "FinalMetricDataList": [
            {"MetricName": "train:rmse", "Value": 88.3},
            {"MetricName": "validation:rmse", "Value": 97.1},
        ],
    }
    assert handler.extract_validation_metric(job_desc) == 97.1


def test_extract_validation_metric_returns_none_when_missing():
    assert handler.extract_validation_metric({"FinalMetricDataList": []}) is None
    assert handler.extract_validation_metric({}) is None


def test_should_promote_when_no_deployed_model():
    assert handler.should_promote(candidate=80.0, deployed=None) is True


def test_should_promote_when_candidate_strictly_better():
    assert handler.should_promote(candidate=80.0, deployed=85.0) is True


def test_should_not_promote_when_candidate_worse_or_equal():
    assert handler.should_promote(candidate=85.0, deployed=85.0) is False
    assert handler.should_promote(candidate=86.0, deployed=85.0) is False


def test_lambda_handler_first_model_promotes(monkeypatch):
    sm = MagicMock()
    sm.describe_training_job.return_value = {
        "FinalMetricDataList": [{"MetricName": "validation:rmse", "Value": 92.0}],
        "ModelArtifacts": {"S3ModelArtifacts": "s3://bkt/training-jobs/run=R/model.tar.gz"},
    }
    s3 = MagicMock()
    # No deployed metrics.json yet.
    s3.get_object.side_effect = s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    def _raise(*a, **k):
        raise s3.exceptions.NoSuchKey()
    s3.get_object.side_effect = _raise

    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "training_job_name": "la-metro-delay-2026-05-27-r1",
        "models_prefix_uri": "s3://bkt/models/",
    }
    out = handler.lambda_handler(event, MagicMock())
    assert out["promote"] is True
    assert out["candidate_metric"] == 92.0
    assert out["deployed_metric"] is None
    assert out["candidate_model_uri"].endswith("/model.tar.gz")


def test_lambda_handler_existing_better_model_blocks_promotion(monkeypatch):
    sm = MagicMock()
    sm.describe_training_job.return_value = {
        "FinalMetricDataList": [{"MetricName": "validation:rmse", "Value": 100.0}],
        "ModelArtifacts": {"S3ModelArtifacts": "s3://bkt/training-jobs/run=R/model.tar.gz"},
    }
    s3 = MagicMock()
    # Deployed model has MAE 90.
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"validation_metric": 90.0}).encode()),
    }
    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    out = handler.lambda_handler(
        {"training_job_name": "j", "models_prefix_uri": "s3://bkt/models/"},
        MagicMock(),
    )
    assert out["promote"] is False
    assert out["candidate_metric"] == 100.0
    assert out["deployed_metric"] == 90.0
