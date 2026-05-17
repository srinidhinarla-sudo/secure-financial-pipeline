"""Singleton SparkSession factory with Delta Lake and AQE configured."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.config import (
    BROADCAST_THRESHOLD_BYTES,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
)


def get_spark(app_name: str = SPARK_APP_NAME, optimized: bool = True) -> SparkSession:
    """Return (or create) a SparkSession configured for Delta Lake.

    Uses configure_spark_with_delta_pip so that the delta-spark JARs installed
    by pip are automatically added to the Spark classpath — no separate Maven
    download or manual --packages flag required.

    Args:
        app_name: Spark application name shown in the UI.
        optimized: When False, disables AQE and caching hints for benchmarking
                   against the unoptimized baseline.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        # Delta Lake extensions — configure_spark_with_delta_pip wires these JARs.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Adaptive Query Execution — lets Spark re-optimize mid-query based on
        # runtime statistics (partition counts, join sizes, skew detection).
        .config("spark.sql.adaptive.enabled", str(optimized).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(optimized).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(optimized).lower())
        # Broadcast join threshold
        .config("spark.sql.autoBroadcastJoinThreshold", BROADCAST_THRESHOLD_BYTES)
        # Reduce default shuffle partitions for a single-node Docker environment
        .config("spark.sql.shuffle.partitions", "8")
        # The Silver table has 38+ columns (V1-V28 consume 28 of the default 32 stat slots).
        # Raise the limit so transaction_date stats are collected, enabling Z-ORDER.
        .config("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "40")
        # Allow Z-ORDER even when stats are sparse (graceful degradation).
        .config("spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled", "false")
    )

    # configure_spark_with_delta_pip injects the pip-installed delta JARs
    # onto the driver/executor classpath so Delta Lake SQL extensions resolve.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    spark.stop()
