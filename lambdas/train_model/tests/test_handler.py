"""Unit tests for the train-model Lambda (Lambda XGBoost training fallback)."""

from __future__ import annotations

import gzip
import io
import pickle
import tarfile

import numpy as np

from lambdas.train_model import handler


def test_parse_training_csv_splits_label_first_column():
    raw = b"100.0,5,8,2,10.0,11.0,12.0,20.5,0.0\n150.0,6,9,3,11.0,12.0,13.0,21.0,0.1\n"
    X, y = handler.parse_training_csv(raw)
    assert y.tolist() == [100.0, 150.0]
    assert X.shape == (2, 8)
    assert X[0].tolist() == [5, 8, 2, 10.0, 11.0, 12.0, 20.5, 0.0]


def test_parse_training_csv_single_row():
    raw = b"100.0,5,8,2,10.0,11.0,12.0,20.5,0.0\n"
    X, y = handler.parse_training_csv(raw)
    assert X.shape == (1, 8)
    assert y.tolist() == [100.0]


def test_split_is_deterministic_and_both_sides_nonempty():
    X = np.arange(40, dtype=float).reshape(10, 4)
    y = np.arange(10, dtype=float)
    Xtr, ytr, Xval, yval = handler.split(X, y, frac=0.8, seed=0)
    assert len(ytr) == 8 and len(yval) == 2
    Xtr2, ytr2, Xval2, yval2 = handler.split(X, y, frac=0.8, seed=0)
    assert np.array_equal(ytr, ytr2) and np.array_equal(yval, yval2)


def test_split_single_row_falls_back_to_train_as_val():
    X = np.array([[1.0, 2.0]])
    y = np.array([3.0])
    Xtr, ytr, Xval, yval = handler.split(X, y)
    assert len(ytr) == 1 and len(yval) == 1
