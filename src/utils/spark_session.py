"""Singleton SparkSession factory with Delta Lake and AQE configured."""

from pyspark.sql import SparkSession

from src.config import (
    BROADCAST_THRESHOLD_BYTES,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
)


def get_spark(app_name: str = SPARK_APP_NAME, optimized: bool = True) -> SparkSession:
    """Return (or create) a SparkSession configured for Delta Lake.

    Args:
        app_name: Spark application name shown in the UI.
        optimized: When False, disables AQE and caching hints for benchmarking
                   against the unoptimized baseline.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        # Delta Lake extensions
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
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    spark.stop()
