"""Bronze layer — raw CSV ingest with schema enforcement and minimal validation."""

from datetime import UTC, date, datetime

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.config import BRONZE_PARTITION_COL, BRONZE_PATH, RAW_CSV_PATH
from src.security.audit import add_row_hash, compute_merkle_root, write_manifest
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="bronze")

# Canonical schema for the Kaggle Credit Card Fraud Detection dataset.
# V1–V28 are PCA-anonymised features; Time is seconds since first transaction.
_FEATURE_FIELDS = [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)]

BRONZE_SCHEMA = StructType(
    [
        StructField("Time", DoubleType(), True),
        *_FEATURE_FIELDS,
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True),  # 0 = legitimate, 1 = fraud
    ]
)


def read_raw_csv(spark: SparkSession, csv_path: str = RAW_CSV_PATH) -> DataFrame:
    """Read the raw CSV enforcing the known schema; drop rows with null mandatory cols."""
    logger.info("Reading raw CSV from %s", csv_path)
    df = spark.read.option("header", "true").schema(BRONZE_SCHEMA).csv(csv_path)
    mandatory = ["Time", "Amount", "Class"]
    df = df.dropna(subset=mandatory)
    logger.info("Raw row count after mandatory-field validation: %d", df.count())
    return df


def add_bronze_metadata(df: DataFrame, ingest_date: date | None = None) -> DataFrame:
    """Attach pipeline metadata columns to every row."""
    today = ingest_date or datetime.now(tz=UTC).date()
    return (
        df.withColumn("ingest_date", F.lit(str(today)))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(RAW_CSV_PATH))
        .withColumn("pipeline_version", F.lit("1.0.0"))
        # Synthetic row ID built from Time + Amount — stable across re-runs.
        .withColumn(
            "row_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("Time").cast(StringType()),
                    F.col("Amount").cast(StringType()),
                ),
                256,
            ),
        )
    )


def write_bronze(df: DataFrame, delta_path: str = BRONZE_PATH) -> None:
    """Write to Bronze Delta table partitioned by ingest_date.

    First run: creates the table.
    Subsequent runs for the SAME ingest_date: MERGE on row_key so the operation
    is fully idempotent (no duplicate rows on re-runs).
    """
    logger.info("Writing Bronze layer to %s", delta_path)

    if DeltaTable.isDeltaTable(df.sparkSession, delta_path):
        bronze_table = DeltaTable.forPath(df.sparkSession, delta_path)
        (
            bronze_table.alias("target")
            .merge(df.alias("source"), "target.row_key = source.row_key")
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Bronze MERGE complete")
    else:
        (
            df.write.format("delta")
            .partitionBy(BRONZE_PARTITION_COL)
            .mode("overwrite")
            .save(delta_path)
        )
        logger.info("Bronze table created (initial load)")


def run_bronze(
    spark: SparkSession,
    csv_path: str = RAW_CSV_PATH,
    delta_path: str = BRONZE_PATH,
    ingest_date: date | None = None,
) -> int:
    """End-to-end Bronze stage. Returns the number of rows written."""
    df_raw = read_raw_csv(spark, csv_path)
    df_meta = add_bronze_metadata(df_raw, ingest_date)
    # Stamp every row with a SHA-256 content hash before writing.
    # This hash covers all financial columns so any post-ingest modification
    # to the stored Parquet files is detectable by the integrity_check DAG.
    df_enriched = add_row_hash(df_meta)
    write_bronze(df_enriched, delta_path)
    count = df_enriched.count()

    # Build Merkle tree over this batch's row hashes and record the root.
    hashes = [r.row_hash for r in df_enriched.select("row_hash").collect()]
    root = compute_merkle_root(hashes)
    write_manifest(spark, layer="bronze", row_count=count, merkle_root=root)

    logger.info("Bronze stage complete — %d rows, merkle_root=%s…", count, root[:16])
    return count
