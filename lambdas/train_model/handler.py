"""Train-model Lambda — Lambda XGBoost training fallback.

Replaces the SageMaker training job (account training quota is 0). Reads the
gzipped CSV the Athena UNLOAD writes to training-sets/run=<id>/, trains XGBoost,
and writes a SageMaker-XGBoost-compatible model.tar.gz to
training-jobs/run=<id>/output/. Returns the validation RMSE + artifact URI so
the existing evaluate/promote states are unchanged.
"""

from __future__ import annotations

import gzip
import io
import logging
import pickle
import tarfile
from typing import Any
from urllib.parse import urlparse

import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Mirrors the hyperparameters in the SageMaker Train state (ml-stack.ts).
HYPERPARAMS = {
    "objective": "reg:squarederror",
    "max_depth": 6,
    "eta": 0.1,
    "subsample": 0.8,
}
NUM_ROUND = 200
METRIC_NAME = "validation:rmse"

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client("s3")
    return _s3_client


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def parse_training_csv(raw: bytes) -> tuple[np.ndarray, np.ndarray]:
    """Header-less CSV, label in column 0; returns (X, y)."""
    arr = np.loadtxt(io.BytesIO(raw), delimiter=",")
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr[:, 1:], arr[:, 0]


def split(
    X: np.ndarray, y: np.ndarray, *, frac: float = 0.8, seed: int = 0
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Deterministic train/validation split. Both sides non-empty when n>=2;
    single-row input evaluates on the training row (portfolio first-model guard).
    """
    n = X.shape[0]
    rng = np.random.RandomState(seed)
    idx = rng.permutation(n)
    if n < 2:
        return X, y, X, y
    cut = min(max(int(n * frac), 1), n - 1)
    tr, va = idx[:cut], idx[cut:]
    return X[tr], y[tr], X[va], y[va]


def train_and_eval(
    Xtr: np.ndarray,
    ytr: np.ndarray,
    Xval: np.ndarray,
    yval: np.ndarray,
    *,
    params: dict | None = None,
    num_round: int = NUM_ROUND,
) -> tuple[Any, float]:
    """Train XGBoost; return (booster, validation RMSE)."""
    import xgboost as xgb

    dtrain = xgb.DMatrix(Xtr, label=ytr)
    dval = xgb.DMatrix(Xval, label=yval)
    booster = xgb.train(
        params or HYPERPARAMS,
        dtrain,
        num_boost_round=num_round,
        evals=[(dval, "validation")],
        verbose_eval=False,
    )
    preds = booster.predict(dval)
    rmse = float(np.sqrt(np.mean((preds - yval) ** 2)))
    return booster, rmse


def package_model(booster: Any) -> bytes:
    """Tar.gz containing the booster pickled as `xgboost-model` — the exact
    layout the SageMaker XGBoost inference container's default model_fn loads.
    """
    model_bytes = pickle.dumps(booster)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="xgboost-model")
        info.size = len(model_bytes)
        tar.addfile(info, io.BytesIO(model_bytes))
    return buf.getvalue()


def _read_training_set(s3, prefix_uri: str) -> bytes:
    """List + read all CSV parts under the prefix; gunzip; concatenate."""
    bucket, prefix = _split_s3(prefix_uri)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parts: list[bytes] = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        parts.append(body.rstrip(b"\n"))
    return b"\n".join(p for p in parts if p) + b"\n"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    training_set_uri = event["training_set_uri"]
    output_model_uri = event["output_model_uri"]

    s3 = _s3()
    raw = _read_training_set(s3, training_set_uri)
    X, y = parse_training_csv(raw)
    Xtr, ytr, Xval, yval = split(X, y)
    booster, rmse = train_and_eval(Xtr, ytr, Xval, yval)
    artifact = package_model(booster)

    out_bucket, out_key = _split_s3(output_model_uri)
    s3.put_object(
        Bucket=out_bucket,
        Key=out_key,
        Body=artifact,
        ContentType="application/x-tar",
    )

    result = {
        "candidate_metric": rmse,
        "candidate_model_uri": output_model_uri,
        "metric_name": METRIC_NAME,
    }
    logger.info(str(result))
    return result
