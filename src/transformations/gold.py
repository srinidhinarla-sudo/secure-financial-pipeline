"""Gold layer — daily and hourly aggregations with Delta MERGE idempotency."""

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import GOLD_DAILY_PATH, GOLD_HOURLY_PATH, SILVER_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="gold")


def read_silver(spark: SparkSession, delta_path: str = SILVER_PATH) -> DataFrame:
    logger.info("Reading Silver table from %s", delta_path)
    return spark.read.format("delta").load(delta_path)


def build_daily_summary(df_silver: DataFrame) -> DataFrame:
    """Aggregate per transaction_date.

    Columns:
    - transaction_date
    - total_transactions
    - fraud_count
    - fraud_rate (0.0–1.0)
    - total_volume (sum of Amount)
    - avg_amount
    - max_amount
    """
    return (
        df_silver.groupBy("transaction_date")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
            (F.sum(F.col("is_fraud").cast("int")) / F.count("*")).alias("fraud_rate"),
            F.sum("Amount").alias("total_volume"),
            F.avg("Amount").alias("avg_amount"),
            F.max("Amount").alias("max_amount"),
        )
        .withColumn("updated_ts", F.current_timestamp())
        .orderBy("transaction_date")
    )


def build_hourly_summary(df_silver: DataFrame) -> DataFrame:
    """Aggregate per transaction_date and transaction_hour.

    Columns:
    - transaction_date
    - transaction_hour
    - total_transactions
    - fraud_count
    - avg_amount
    - amount_bucket (dominant bucket by transaction count)
    """
    # Dominant bucket per (date, hour) using a sub-aggregation then broadcast join.
    # The bucket_counts DataFrame is small (at most 5 buckets × days × 24 hours)
    # so Spark will automatically broadcast it given the configured threshold.
    bucket_counts = df_silver.groupBy("transaction_date", "transaction_hour", "amount_bucket").agg(
        F.count("*").alias("bucket_tx_count")
    )

    from pyspark.sql.window import Window

    rank_window = Window.partitionBy("transaction_date", "transaction_hour").orderBy(
        F.col("bucket_tx_count").desc()
    )
    dominant_bucket = (
        bucket_counts.withColumn("rn", F.row_number().over(rank_window))
        .filter(F.col("rn") == 1)
        .drop("rn", "bucket_tx_count")
    )

    base = (
        df_silver.groupBy("transaction_date", "transaction_hour")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
            F.avg("Amount").alias("avg_amount"),
        )
        .withColumn("updated_ts", F.current_timestamp())
    )

    # Broadcast join: dominant_bucket is tiny relative to the Silver table.
    # Spark honours autoBroadcastJoinThreshold, but we hint explicitly to make
    # intent visible and guarantee the optimisation regardless of table stats.
    return base.join(
        F.broadcast(dominant_bucket),
        on=["transaction_date", "transaction_hour"],
        how="left",
    ).orderBy("transaction_date", "transaction_hour")


def _merge_or_create(
    spark: SparkSession,
    df: DataFrame,
    delta_path: str,
    merge_keys: list[str],
) -> None:
    """Generic helper: MERGE df into an existing Delta table, or create it."""
    if DeltaTable.isDeltaTable(spark, delta_path):
        table = DeltaTable.forPath(spark, delta_path)
        condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
        (
            table.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Gold MERGE complete for %s", delta_path)
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)
        logger.info("Gold table created (initial load) at %s", delta_path)


def run_gold(
    spark: SparkSession,
    silver_path: str = SILVER_PATH,
    daily_path: str = GOLD_DAILY_PATH,
    hourly_path: str = GOLD_HOURLY_PATH,
) -> dict[str, int]:
    """End-to-end Gold stage. Returns row counts keyed by table name."""
    df_silver = read_silver(spark, silver_path)

    # Cache: both daily and hourly aggregations scan the full Silver table.
    # A single cache pass avoids reading the Delta files twice.
    df_silver.cache()
    logger.info("Silver DataFrame cached for Gold aggregations")

    df_daily = build_daily_summary(df_silver)
    _merge_or_create(spark, df_daily, daily_path, merge_keys=["transaction_date"])

    df_hourly = build_hourly_summary(df_silver)
    _merge_or_create(
        spark, df_hourly, hourly_path, merge_keys=["transaction_date", "transaction_hour"]
    )

    daily_count = df_daily.count()
    hourly_count = df_hourly.count()

    df_silver.unpersist()
    logger.info("Gold stage complete — daily=%d rows, hourly=%d rows", daily_count, hourly_count)
    return {"daily": daily_count, "hourly": hourly_count}
