"""Gold layer — unsupervised fraud anomaly detection via Isolation Forest.

Trains an Isolation Forest on the 28 PCA feature columns (V1-V28) plus Amount
from the Silver table.  The model is entirely unsupervised: it never sees the
Class label during training, which mirrors a real production scenario where
ground-truth fraud labels arrive days or weeks after the transaction clears.

After scoring every transaction the results are written to the Gold
fraud_signals Delta table.  Precision, recall, and F1 are computed against the
actual labels purely for evaluation — they are not used by the model.

Why Isolation Forest?
  Random subsampling + random feature splits make anomalies (fraud) isolable in
  far fewer splits than normal transactions.  The algorithm runs in O(n log n)
  and scales to millions of rows without the memory overhead of distance-based
  methods like LOF or k-NN.
"""

from __future__ import annotations

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_recall_fscore_support

from src.config import GOLD_FRAUD_SIGNALS_PATH, SILVER_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="anomaly")

# All features fed to the model — PCA components + raw amount.
# Class (label) is intentionally excluded from training.
FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount"]

# Contamination slightly above the true 0.172% fraud rate so the model
# errs on the side of flagging rather than missing fraud.
_CONTAMINATION = 0.002


def run_anomaly_detection(spark: SparkSession) -> dict:
    """Train Isolation Forest on Silver data and write fraud_signals to Gold.

    Returns a metrics dict with precision, recall, F1, and counts so the
    calling DAG task can XCom-push them for downstream visibility.
    """
    logger.info("Loading Silver table for anomaly detection from %s", SILVER_PATH)
    df_silver = spark.read.format("delta").load(SILVER_PATH)

    # Collect to pandas — 280K rows × 30 cols ≈ 65 MB, well within driver memory.
    cols_needed = FEATURE_COLS + ["is_fraud", "row_key"]
    pdf = df_silver.select(*cols_needed).toPandas()
    pdf["is_fraud"] = pdf["is_fraud"].astype(bool)

    logger.info(
        "Training Isolation Forest on %d transactions (%d features)…", len(pdf), len(FEATURE_COLS)
    )

    X = pdf[FEATURE_COLS].to_numpy(dtype=np.float64)

    clf = IsolationForest(
        n_estimators=200,
        contamination=_CONTAMINATION,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X)

    # decision_function: lower score → more anomalous (negative = outlier)
    raw_scores = clf.decision_function(X)
    predictions = clf.predict(X)  # -1 = anomaly, 1 = normal

    pdf["anomaly_score"] = raw_scores.astype(float)
    pdf["is_predicted_fraud"] = predictions == -1

    # ── Evaluation (labels used only here, never during training) ─────────────
    y_true = pdf["is_fraud"].astype(int).to_numpy()
    y_pred = pdf["is_predicted_fraud"].astype(int).to_numpy()

    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0
    )

    flagged = int(pdf["is_predicted_fraud"].sum())
    actual_fraud = int(pdf["is_fraud"].sum())
    caught = int((pdf["is_predicted_fraud"] & pdf["is_fraud"]).sum())

    logger.info(
        "Isolation Forest results — flagged=%d actual_fraud=%d caught=%d "
        "precision=%.3f recall=%.3f f1=%.3f",
        flagged,
        actual_fraud,
        caught,
        precision,
        recall,
        f1,
    )

    # ── Write fraud_signals Gold table ────────────────────────────────────────
    schema = StructType(
        [
            StructField("row_key", StringType(), False),
            StructField("anomaly_score", DoubleType(), False),
            StructField("is_predicted_fraud", BooleanType(), False),
            StructField("is_actual_fraud", BooleanType(), False),
        ]
    )

    signals_pdf = pdf[["row_key", "anomaly_score", "is_predicted_fraud", "is_fraud"]].rename(
        columns={"is_fraud": "is_actual_fraud"}
    )

    (
        spark.createDataFrame(signals_pdf, schema=schema)
        .write.format("delta")
        .mode("overwrite")
        .save(GOLD_FRAUD_SIGNALS_PATH)
    )
    logger.info("fraud_signals written to %s", GOLD_FRAUD_SIGNALS_PATH)

    return {
        "total_transactions": len(pdf),
        "flagged_as_anomaly": flagged,
        "actual_fraud_in_dataset": actual_fraud,
        "fraud_caught_by_model": caught,
        "precision": round(float(precision), 3),
        "recall": round(float(recall), 3),
        "f1": round(float(f1), 3),
    }
