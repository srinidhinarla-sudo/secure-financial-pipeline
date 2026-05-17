"""Unit tests for the Silver transformation layer."""

from datetime import date

import pytest
from pyspark.sql import Row, SparkSession
from src.transformations.bronze import BRONZE_SCHEMA, add_bronze_metadata
from src.transformations.silver import deduplicate, enrich


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestSilver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_df(spark):
    """Three rows: two legitimate, one fraud, one duplicate of row 1."""
    features = {f"V{i}": float(i) for i in range(1, 29)}
    rows = [
        Row(Time=0.0, **features, Amount=149.62, Class=0),
        Row(Time=86400.0, **features, Amount=2.69, Class=1),  # 1 day later
        Row(Time=0.0, **features, Amount=149.62, Class=0),  # duplicate of row 0
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    return add_bronze_metadata(df, ingest_date=date(2024, 1, 1))


# ── test 6 ──────────────────────────────────────────────────────────────────
def test_deduplication_removes_duplicates(bronze_df):
    """After deduplication, the duplicate row (row 0 copy) must be gone."""
    deduped = deduplicate(bronze_df)
    assert deduped.count() == 2


# ── test 7 ──────────────────────────────────────────────────────────────────
def test_enrich_adds_transaction_date(bronze_df):
    """transaction_date must be present and parseable as a date."""
    enriched = enrich(deduplicate(bronze_df))
    assert "transaction_date" in enriched.columns
    dates = [r.transaction_date for r in enriched.select("transaction_date").collect()]
    assert all(d is not None for d in dates)


# ── test 8 ──────────────────────────────────────────────────────────────────
def test_enrich_amount_bucket_correctness(spark):
    """Amount 5.0 → '0-10', 75.0 → '50-200', 500.0 → '200-1000', 5000.0 → '1000+'."""
    features = {f"V{i}": 0.0 for i in range(1, 29)}
    rows = [
        Row(Time=0.0, **features, Amount=5.0, Class=0),
        Row(Time=1.0, **features, Amount=75.0, Class=0),
        Row(Time=2.0, **features, Amount=500.0, Class=0),
        Row(Time=3.0, **features, Amount=5000.0, Class=1),
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df = add_bronze_metadata(df)
    enriched = enrich(df)
    rows = enriched.select("Amount", "amount_bucket").collect()
    buckets = {r.Amount: r.amount_bucket for r in rows}
    assert buckets[5.0] == "0-10"
    assert buckets[75.0] == "50-200"
    assert buckets[500.0] == "200-1000"
    assert buckets[5000.0] == "1000+"


# ── test 9 ──────────────────────────────────────────────────────────────────
def test_is_fraud_column(bronze_df):
    """is_fraud must be True for Class==1 rows only."""
    enriched = enrich(deduplicate(bronze_df))
    fraud_rows = enriched.filter("is_fraud = true").collect()
    legit_rows = enriched.filter("is_fraud = false").collect()
    assert all(r.Class == 1 for r in fraud_rows)
    assert all(r.Class == 0 for r in legit_rows)


# ── test 10 ─────────────────────────────────────────────────────────────────
def test_amount_log1p_is_non_negative(bronze_df):
    """log1p(Amount) must be ≥ 0 for all non-negative amounts."""
    enriched = enrich(deduplicate(bronze_df))
    neg_log = enriched.filter("amount_log1p < 0").count()
    assert neg_log == 0
