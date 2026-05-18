"""Run the Isolation Forest anomaly detection demo and print results.

Trains an unsupervised Isolation Forest on 279K+ credit card transactions
(V1-V28 PCA features + Amount) without ever seeing fraud labels during training.
Prints precision, recall, and F1 evaluated post-hoc against ground-truth labels.

Usage:
    python scripts/run_anomaly_demo.py

Prerequisites:
    Run the full pipeline first: make run
"""

from __future__ import annotations

import os

os.environ.setdefault("PIPELINE_DATA_DIR", os.path.abspath("data"))
os.environ.setdefault("PIPELINE_DELTA_DIR", os.path.abspath("data/delta"))
os.environ.setdefault("SLACK_WEBHOOK_URL", "")

from src.transformations.anomaly import run_anomaly_detection  # noqa: E402
from src.utils.spark_session import get_spark, stop_spark  # noqa: E402

_SEP = "=" * 62


def main() -> None:
    print(f"\n{_SEP}")
    print("  ISOLATION FOREST — Unsupervised Fraud Anomaly Detection")
    print(_SEP)
    print("\nFeatures: V1–V28 (PCA) + Amount  |  Class label: NEVER seen during training")
    print("Dataset : 284,807 credit card transactions (Kaggle ULB)\n")

    spark = get_spark(app_name="AnomalyDemo")
    try:
        metrics = run_anomaly_detection(spark)
    finally:
        stop_spark(spark)

    print(f"\n{_SEP}")
    print("  RESULTS")
    print(_SEP)
    print(f"  Total transactions    : {metrics['total_transactions']:,}")
    print(f"  Actual fraud cases    : {metrics['actual_fraud_in_dataset']:,}")
    print(f"  Flagged as anomaly    : {metrics['flagged_as_anomaly']:,}")
    print(f"  Fraud caught          : {metrics['fraud_caught_by_model']:,}")
    print(f"  Precision             : {metrics['precision']:.3f}")
    print(f"  Recall                : {metrics['recall']:.3f}")
    print(f"  F1                    : {metrics['f1']:.3f}")
    print("\n  fraud_signals written to data/delta/gold/fraud_signals")
    print(f"{_SEP}\n")
    print("Note: precision/recall reflect an unsupervised model with no label")
    print("leakage. In production, fraud labels arrive days after transactions")
    print("clear — supervised models cannot be trained in real time.")


if __name__ == "__main__":
    main()
