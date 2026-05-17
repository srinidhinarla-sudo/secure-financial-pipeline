"""Unit tests for the Gold aggregation layer."""

from datetime import date

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.transformations.gold import build_daily_summary, build_hourly_summary

# Minimal Silver schema for building Gold-layer test fixtures
SILVER_SCHEMA = StructType(
    [
        StructField("transaction_date", DateType(), True),
        StructField("transaction_hour", IntegerType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("amount_bucket", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("Class", IntegerType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestGold")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def silver_df(spark):
    d1 = date(2013, 9, 1)
    d2 = date(2013, 9, 2)
    rows = [
        Row(
            transaction_date=d1,
            transaction_hour=9,
            Amount=100.0,
            amount_bucket="50-200",
            is_fraud=False,
            Class=0,
        ),
        Row(
            transaction_date=d1,
            transaction_hour=9,
            Amount=200.0,
            amount_bucket="200-1000",
            is_fraud=True,
            Class=1,
        ),
        Row(
            transaction_date=d1,
            transaction_hour=14,
            Amount=50.0,
            amount_bucket="10-50",
            is_fraud=False,
            Class=0,
        ),
        Row(
            transaction_date=d2,
            transaction_hour=10,
            Amount=30.0,
            amount_bucket="10-50",
            is_fraud=False,
            Class=0,
        ),
    ]
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


# ── test 11 ─────────────────────────────────────────────────────────────────
def test_daily_summary_row_count(silver_df):
    """daily_summary must have exactly one row per transaction_date."""
    daily = build_daily_summary(silver_df)
    assert daily.count() == 2


# ── test 12 ─────────────────────────────────────────────────────────────────
def test_daily_summary_fraud_count(silver_df):
    """Date 2013-09-01 has 3 transactions, 1 fraud."""
    daily = build_daily_summary(silver_df)
    row = daily.filter("transaction_date = '2013-09-01'").collect()[0]
    assert row.total_transactions == 3
    assert row.fraud_count == 1


# ── test 13 ─────────────────────────────────────────────────────────────────
def test_daily_summary_fraud_rate(silver_df):
    """Fraud rate for 2013-09-01 must be 1/3 ≈ 0.333."""
    daily = build_daily_summary(silver_df)
    row = daily.filter("transaction_date = '2013-09-01'").collect()[0]
    assert abs(row.fraud_rate - 1 / 3) < 1e-6


# ── test 14 ─────────────────────────────────────────────────────────────────
def test_hourly_summary_has_amount_bucket(silver_df):
    """hourly_summary must include the amount_bucket column from the broadcast join."""
    hourly = build_hourly_summary(silver_df)
    assert "amount_bucket" in hourly.columns


# ── test 15 ─────────────────────────────────────────────────────────────────
def test_daily_total_volume(silver_df):
    """Total volume for 2013-09-01 = 100 + 200 + 50 = 350."""
    daily = build_daily_summary(silver_df)
    row = daily.filter("transaction_date = '2013-09-01'").collect()[0]
    assert abs(row.total_volume - 350.0) < 1e-6
