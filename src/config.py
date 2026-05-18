"""Central configuration — all paths and tuning constants loaded from env."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", str(BASE_DIR / "data")))
DELTA_DIR = Path(os.getenv("PIPELINE_DELTA_DIR", str(DATA_DIR / "delta")))

RAW_CSV_PATH = str(DATA_DIR / "creditcard.csv")

BRONZE_PATH = str(DELTA_DIR / "bronze" / "transactions")
SILVER_PATH = str(DELTA_DIR / "silver" / "transactions")
GOLD_DAILY_PATH = str(DELTA_DIR / "gold" / "daily_summary")
GOLD_HOURLY_PATH = str(DELTA_DIR / "gold" / "hourly_summary")
AUDIT_PATH = str(DELTA_DIR / "_audit" / "manifests")
GOLD_FRAUD_SIGNALS_PATH = str(DELTA_DIR / "gold" / "fraud_signals")

# ── Slack ──────────────────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL: str | None = os.getenv("SLACK_WEBHOOK_URL")

# ── PySpark ───────────────────────────────────────────────────────────────────
SPARK_APP_NAME = "SecureFinancialPipeline"
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

# ── Pipeline constants ────────────────────────────────────────────────────────
BRONZE_PARTITION_COL = "ingest_date"
# Partition by load batch so Z-ORDER targets transaction_date (data col, not partition col)
SILVER_PARTITION_COL = "ingest_date"

# Broadcast join size threshold (bytes).  Tables smaller than this are broadcast.
BROADCAST_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB

# Silver unoptimized mode: set env var to "1" for benchmarking baseline.
SILVER_UNOPTIMIZED = os.getenv("SILVER_UNOPTIMIZED", "0") == "1"
