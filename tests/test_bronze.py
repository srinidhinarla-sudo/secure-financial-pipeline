"""Unit tests for the Bronze transformation layer."""

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from src.transformations.bronze import (
    BRONZE_SCHEMA,
    add_bronze_metadata,
)


@pytest.fixture(scope="module")
def spark():
    """Minimal SparkSession for tests — no Delta extensions needed here."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestBronze")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_df(spark):
    """Two valid rows matching the Kaggle schema."""
    feature_values = {f"V{i}": float(i) for i in range(1, 29)}
    rows = [
        Row(Time=0.0, **feature_values, Amount=149.62, Class=0),
        Row(Time=1.0, **feature_values, Amount=2.69, Class=1),
    ]
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


# ── test 1 ──────────────────────────────────────────────────────────────────
def test_bronze_schema_field_count():
    """Schema must have exactly 31 fields: Time, V1–V28, Amount, Class."""
    assert len(BRONZE_SCHEMA.fields) == 31


# ── test 2 ──────────────────────────────────────────────────────────────────
def test_bronze_schema_types():
    field_map = {f.name: f.dataType for f in BRONZE_SCHEMA.fields}
    assert isinstance(field_map["Time"], DoubleType)
    assert isinstance(field_map["Amount"], DoubleType)
    assert isinstance(field_map["Class"], IntegerType)


# ── test 3 ──────────────────────────────────────────────────────────────────
def test_add_bronze_metadata_adds_columns(spark, sample_bronze_df):
    """Metadata enrichment must add the 5 expected pipeline columns."""
    enriched = add_bronze_metadata(sample_bronze_df)
    col_names = enriched.columns
    for col in ("ingest_date", "ingest_ts", "source_file", "pipeline_version", "row_key"):
        assert col in col_names, f"Missing column: {col}"


# ── test 4 ──────────────────────────────────────────────────────────────────
def test_row_key_is_deterministic(spark, sample_bronze_df):
    """The same input row must always produce the same row_key."""
    df1 = add_bronze_metadata(sample_bronze_df)
    df2 = add_bronze_metadata(sample_bronze_df)
    keys1 = sorted([r.row_key for r in df1.select("row_key").collect()])
    keys2 = sorted([r.row_key for r in df2.select("row_key").collect()])
    assert keys1 == keys2


# ── test 5 ──────────────────────────────────────────────────────────────────
def test_null_mandatory_fields_are_dropped(spark):
    """Rows with null Time, Amount, or Class must be filtered out."""
    rows = [
        Row(Time=None, **{f"V{i}": 0.0 for i in range(1, 29)}, Amount=10.0, Class=0),
        Row(Time=1.0, **{f"V{i}": 0.0 for i in range(1, 29)}, Amount=None, Class=0),
        Row(Time=2.0, **{f"V{i}": 0.0 for i in range(1, 29)}, Amount=5.0, Class=None),
        Row(Time=3.0, **{f"V{i}": 0.0 for i in range(1, 29)}, Amount=100.0, Class=1),
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df_valid = df.dropna(subset=["Time", "Amount", "Class"])
    assert df_valid.count() == 1
