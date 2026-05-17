"""Tests for the tamper-evident audit trail (src/security/audit.py)."""

import hashlib

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.security.audit import (
    _CONTENT_COLS,
    add_row_hash,
    compute_merkle_root,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_audit")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── compute_merkle_root ────────────────────────────────────────────────────────


def test_merkle_root_empty():
    assert compute_merkle_root([]) == ""


def test_merkle_root_single_leaf():
    assert compute_merkle_root(["abc"]) == "abc"


def test_merkle_root_is_deterministic():
    hashes = ["aaa", "bbb", "ccc", "ddd"]
    assert compute_merkle_root(hashes) == compute_merkle_root(hashes)


def test_merkle_root_order_independent():
    """Root must be the same regardless of input order (we sort internally)."""
    h = ["aaa", "bbb", "ccc"]
    assert compute_merkle_root(h) == compute_merkle_root(list(reversed(h)))


def test_merkle_root_two_leaves():
    a, b = "aaa", "bbb"
    expected = hashlib.sha256((a + b).encode()).hexdigest()
    # sorted(["aaa","bbb"]) == ["aaa","bbb"] so order is predictable here
    assert compute_merkle_root([a, b]) == expected


def test_merkle_root_odd_leaves_duplicates_last():
    """Odd-length layer should duplicate the last node — not drop it."""
    root_three = compute_merkle_root(["aaa", "bbb", "ccc"])
    # With three leaves, "ccc" is duplicated: tree has two pairs
    # pair1: sha256(aaa+bbb), pair2: sha256(ccc+ccc)
    p1 = hashlib.sha256(("aaa" + "bbb").encode()).hexdigest()
    p2 = hashlib.sha256(("ccc" + "ccc").encode()).hexdigest()
    expected = hashlib.sha256((p1 + p2).encode()).hexdigest()
    assert root_three == expected


def test_different_inputs_produce_different_roots():
    assert compute_merkle_root(["aaa", "bbb"]) != compute_merkle_root(["aaa", "ccc"])


# ── add_row_hash ───────────────────────────────────────────────────────────────


def _make_row(spark, time=0.0, amount=100.0, cls=0):
    data = {
        "Time": time,
        **{f"V{i}": float(i) for i in range(1, 29)},
        "Amount": amount,
        "Class": cls,
    }
    return spark.createDataFrame([data])


def test_add_row_hash_column_exists(spark):
    df = add_row_hash(_make_row(spark))
    assert "row_hash" in df.columns


def test_add_row_hash_is_64_hex_chars(spark):
    df = add_row_hash(_make_row(spark))
    h = df.select("row_hash").first().row_hash
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_add_row_hash_is_deterministic(spark):
    h1 = add_row_hash(_make_row(spark)).select("row_hash").first().row_hash
    h2 = add_row_hash(_make_row(spark)).select("row_hash").first().row_hash
    assert h1 == h2


def test_add_row_hash_changes_when_content_changes(spark):
    h_legit = add_row_hash(_make_row(spark, cls=0)).select("row_hash").first().row_hash
    h_fraud = add_row_hash(_make_row(spark, cls=1)).select("row_hash").first().row_hash
    assert h_legit != h_fraud


def test_tamper_detection_catches_modified_row(spark):
    """Simulates a tampered row: stored hash no longer matches recomputed hash."""
    df = add_row_hash(_make_row(spark, amount=50.0))

    # Overwrite row_hash with a wrong value (simulating file tampering)
    df_tampered = df.withColumn("row_hash", F.lit("deadbeef" * 8))

    mismatch_count = (
        df_tampered.withColumn(
            "_recomputed",
            F.sha2(
                F.concat_ws("|", *[F.col(c).cast("string") for c in _CONTENT_COLS]),
                256,
            ),
        )
        .filter(F.col("row_hash") != F.col("_recomputed"))
        .count()
    )
    assert mismatch_count == 1


def test_untampered_row_passes_check(spark):
    """A row whose hash was computed correctly should pass integrity check."""
    df = add_row_hash(_make_row(spark, amount=99.0))

    mismatch_count = (
        df.withColumn(
            "_recomputed",
            F.sha2(
                F.concat_ws("|", *[F.col(c).cast("string") for c in _CONTENT_COLS]),
                256,
            ),
        )
        .filter(F.col("row_hash") != F.col("_recomputed"))
        .count()
    )
    assert mismatch_count == 0
