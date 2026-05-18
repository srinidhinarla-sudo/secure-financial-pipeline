"""Tests for the Isolation Forest anomaly detection (src/transformations/anomaly.py)."""

import numpy as np
from sklearn.ensemble import IsolationForest
from src.transformations.anomaly import _CONTAMINATION, FEATURE_COLS


def _make_dataset(n_normal: int = 300, n_fraud: int = 10) -> tuple:
    """Return (X, y) with separable normal and anomalous points."""
    rng = np.random.default_rng(0)
    X_normal = rng.standard_normal((n_normal, len(FEATURE_COLS)))
    X_fraud = rng.standard_normal((n_fraud, len(FEATURE_COLS))) * 3 + 5
    X = np.vstack([X_normal, X_fraud])
    y = np.array([0] * n_normal + [1] * n_fraud)
    return X, y


def test_feature_cols_count():
    assert len(FEATURE_COLS) == 29  # V1-V28 + Amount


def test_feature_cols_includes_amount():
    assert "Amount" in FEATURE_COLS


def test_feature_cols_excludes_class_label():
    assert "Class" not in FEATURE_COLS
    assert "is_fraud" not in FEATURE_COLS


def test_contamination_above_actual_fraud_rate():
    actual_fraud_rate = 492 / 284_807
    assert _CONTAMINATION > actual_fraud_rate


def test_isolation_forest_detects_obvious_anomalies():
    """Model should flag most of the well-separated anomalous points."""
    X, y = _make_dataset()
    # Set contamination to match the actual fraud fraction in the test dataset
    # so the model has budget to flag all anomalous points.
    fraud_rate = float((y == 1).sum()) / len(y)
    clf = IsolationForest(contamination=fraud_rate, random_state=42)
    preds = clf.fit_predict(X)
    flagged = preds == -1

    # Well-separated anomalies (mean shifted by 5σ) should be caught at high recall
    fraud_mask = y == 1
    recall = flagged[fraud_mask].sum() / fraud_mask.sum()
    assert recall >= 0.70, f"Recall {recall:.2f} below threshold"


def test_isolation_forest_anomaly_scores_are_finite():
    X, _ = _make_dataset()
    clf = IsolationForest(contamination=_CONTAMINATION, random_state=42)
    clf.fit(X)
    scores = clf.decision_function(X)
    assert np.all(np.isfinite(scores))


def test_anomaly_scores_lower_for_fraud():
    """Fraud rows (shifted distribution) should on average score lower than normal rows."""
    X, y = _make_dataset(n_normal=500, n_fraud=20)
    clf = IsolationForest(contamination=_CONTAMINATION, random_state=42)
    clf.fit(X)
    scores = clf.decision_function(X)
    mean_normal = scores[y == 0].mean()
    mean_fraud = scores[y == 1].mean()
    assert (
        mean_fraud < mean_normal
    ), f"Expected fraud scores ({mean_fraud:.3f}) < normal scores ({mean_normal:.3f})"


def test_model_is_deterministic_with_fixed_seed():
    X, _ = _make_dataset()
    clf1 = IsolationForest(contamination=_CONTAMINATION, random_state=42)
    clf2 = IsolationForest(contamination=_CONTAMINATION, random_state=42)
    s1 = clf1.fit(X).decision_function(X)
    s2 = clf2.fit(X).decision_function(X)
    np.testing.assert_array_equal(s1, s2)
