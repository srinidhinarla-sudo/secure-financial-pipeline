"""Run the full Bronze → Silver → Gold pipeline locally (no Airflow/Docker needed)."""

import os
import time

os.environ.setdefault("PIPELINE_DATA_DIR", os.path.abspath("data"))
os.environ.setdefault("PIPELINE_DELTA_DIR", os.path.abspath("data/delta"))
os.environ.setdefault("SLACK_WEBHOOK_URL", "")

from src.transformations.bronze import run_bronze
from src.transformations.gold import run_gold
from src.transformations.silver import run_silver
from src.utils.logging_config import get_logger
from src.utils.spark_session import get_spark

logger = get_logger("pipeline_runner", stage="runner")


def hms(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def main():
    print("\n" + "=" * 60)
    print("  Secure Financial Data Pipeline — Local Run")
    print("=" * 60)

    spark = get_spark(app_name="PipelineLocalRun", optimized=True)
    wall_start = time.time()

    # ── Bronze ────────────────────────────────────────────────────────────────
    print("\n[1/3] Bronze — raw CSV ingest")
    t0 = time.time()
    bronze_count = run_bronze(spark)
    bronze_time = time.time() - t0
    print(f"      ✓ {bronze_count:,} rows  ({hms(bronze_time)})")

    # ── Silver ────────────────────────────────────────────────────────────────
    print("\n[2/3] Silver — deduplicate, enrich, Z-ORDER")
    t0 = time.time()
    silver_count = run_silver(spark, optimized=True)
    silver_time = time.time() - t0
    print(f"      ✓ {silver_count:,} rows  ({hms(silver_time)})")

    # ── Gold ──────────────────────────────────────────────────────────────────
    print("\n[3/3] Gold — daily & hourly aggregations")
    t0 = time.time()
    gold_counts = run_gold(spark)
    gold_time = time.time() - t0
    print(f"      ✓ daily={gold_counts['daily']} rows, hourly={gold_counts['hourly']} rows  ({hms(gold_time)})")

    total_time = time.time() - wall_start
    print("\n" + "=" * 60)
    print(f"  Pipeline complete in {hms(total_time)}")
    print("=" * 60)

    # ── Spot-check Gold output ────────────────────────────────────────────────
    from src.config import GOLD_DAILY_PATH

    print("\n  Gold daily_summary (top 10 rows by fraud_count desc):")
    spark.read.format("delta").load(GOLD_DAILY_PATH) \
        .orderBy("fraud_count", ascending=False) \
        .select("transaction_date", "total_transactions", "fraud_count", "fraud_rate", "total_volume") \
        .show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
