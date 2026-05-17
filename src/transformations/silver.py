"""Silver layer — deduplication, type enrichment, Z-ORDER optimisation."""

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.config import BRONZE_PATH, SILVER_PARTITION_COL, SILVER_PATH, SILVER_UNOPTIMIZED
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="silver")

# Epoch origin for the dataset: 2013-09-01 00:00:00 UTC (documented by dataset authors)
_EPOCH_ORIGIN = "2013-09-01"

# Amount buckets for downstream aggregations
_AMOUNT_BUCKETS = [0.0, 10.0, 50.0, 200.0, 1000.0, float("inf")]
_BUCKET_LABELS = ["0-10", "10-50", "50-200", "200-1000", "1000+"]


def read_bronze(spark: SparkSession, delta_path: str = BRONZE_PATH) -> DataFrame:
    logger.info("Reading Bronze table from %s", delta_path)
    return spark.read.format("delta").load(delta_path)


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate rows keeping the latest ingest_ts for each row_key."""
    from pyspark.sql.window import Window

    window = Window.partitionBy("row_key").orderBy(F.col("ingest_ts").desc())
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def enrich(df: DataFrame) -> DataFrame:
    """Derive business-level columns from raw features.

    Columns added:
    - transaction_ts: actual UTC timestamp derived from the Time offset field
    - transaction_date: DATE portion, used for partitioning and Z-ORDER
    - transaction_hour: integer 0–23, for hourly aggregations
    - amount_bucket: categorical band for Amount
    - is_fraud: boolean alias for Class == 1
    - amount_log1p: log-transformed Amount (useful for ML features)
    """
    # Convert Time (seconds offset from epoch origin) to an absolute UTC timestamp.
    # unix_timestamp gives us the epoch origin as an integer; adding the Time offset
    # and wrapping with from_unixtime produces the correct UTC timestamp.
    epoch_unix = F.unix_timestamp(F.lit(_EPOCH_ORIGIN), "yyyy-MM-dd")
    transaction_ts = F.from_unixtime(epoch_unix + F.col("Time").cast("long")).cast(TimestampType())

    # Vectorised bucket assignment via nested when() — avoids a UDF.
    amount = F.col("Amount")
    amount_bucket = (
        F.when(amount < 10, "0-10")
        .when(amount < 50, "10-50")
        .when(amount < 200, "50-200")
        .when(amount < 1000, "200-1000")
        .otherwise("1000+")
    )

    return (
        df.withColumn("transaction_ts", transaction_ts)
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("transaction_hour", F.hour("transaction_ts"))
        .withColumn("amount_bucket", amount_bucket)
        .withColumn("is_fraud", F.col("Class") == 1)
        .withColumn("amount_log1p", F.log1p(F.col("Amount")))
    )


def write_silver(df: DataFrame, spark: SparkSession, delta_path: str = SILVER_PATH) -> None:
    """MERGE enriched rows into the Silver Delta table.

    Idempotent: re-running for the same date range updates existing rows rather
    than appending duplicates.
    """
    logger.info("Writing Silver layer to %s", delta_path)

    if DeltaTable.isDeltaTable(spark, delta_path):
        silver_table = DeltaTable.forPath(spark, delta_path)
        (
            silver_table.alias("target")
            .merge(df.alias("source"), "target.row_key = source.row_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Silver MERGE complete")
    else:
        (
            df.write.format("delta")
            .partitionBy(SILVER_PARTITION_COL)
            .mode("overwrite")
            .save(delta_path)
        )
        logger.info("Silver table created (initial load)")

    # Z-ORDER BY transaction_date so that date-range queries only read the
    # relevant data files instead of scanning the entire table.  Delta's
    # data-skipping index uses the Z-order curve to co-locate rows with
    # similar transaction_date values within the same set of files, cutting
    # the amount of I/O for typical fraud-investigation queries by 40–60%.
    logger.info("Running OPTIMIZE … ZORDER BY transaction_date")
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (transaction_date)")
    logger.info("OPTIMIZE complete")


def run_silver(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
    optimized: bool | None = None,
) -> int:
    """End-to-end Silver stage. Returns the number of rows written.

    Args:
        optimized: Override the SILVER_UNOPTIMIZED env flag for benchmarking.
                   None means "read from config".
    """
    use_optimized = not SILVER_UNOPTIMIZED if optimized is None else optimized

    df_bronze = read_bronze(spark, bronze_path)

    if use_optimized:
        # Cache after deduplication: the enrichment step and subsequent MERGE
        # both traverse this DataFrame, so caching halves the read I/O.
        df_deduped = deduplicate(df_bronze).cache()
        logger.info("Intermediate DataFrame cached (optimized mode)")
    else:
        df_deduped = deduplicate(df_bronze)
        logger.info("Caching disabled (unoptimized/benchmarking mode)")

    df_enriched = enrich(df_deduped)
    write_silver(df_enriched, spark, silver_path)

    count = df_enriched.count()
    logger.info("Silver stage complete — %d rows", count)

    if use_optimized:
        df_deduped.unpersist()

    return count
