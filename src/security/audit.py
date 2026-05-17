"""Tamper-evident audit trail for the financial pipeline.

Every Bronze ingest batch:
  1. Stamps each row with a SHA-256 content hash (row_hash) covering all
     financial columns — distinct from the row_key dedup key.
  2. Collects those hashes and builds a Merkle tree; the root is stored in
     the _audit/manifests Delta table alongside the row count and batch ID.

A separate integrity_check DAG later:
  1. Re-reads the live Bronze table and recomputes every row_hash from scratch.
  2. Flags any row whose stored hash no longer matches its recomputed hash
     (indicating in-place modification of the Delta files).
  3. Recomputes the Merkle root from current hashes and compares it to the
     value recorded at ingest time (detects deletions or insertions too).
  4. Fires a Slack alert if either check fails.

Why Merkle trees?
  A single root hash lets you prove that ANY row in a 284K-record batch is
  unchanged without re-hashing the entire table — you only need O(log n)
  sibling hashes along the path from the leaf to the root.  The same
  construction underpins Bitcoin, certificate transparency logs (RFC 6962),
  and Git's object store.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import AUDIT_PATH, BRONZE_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__, stage="audit")

# All financial content columns — everything except pipeline metadata.
# Changing this list is a breaking change: stored hashes become incompatible.
_CONTENT_COLS = [
    "Time",
    *[f"V{i}" for i in range(1, 29)],
    "Amount",
    "Class",
]

_MANIFEST_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("merkle_root", StringType(), False),
        StructField("recorded_at", TimestampType(), False),
    ]
)


# ── Row-level hashing ──────────────────────────────────────────────────────────


def add_row_hash(df: DataFrame) -> DataFrame:
    """Append a row_hash column: SHA-256 of all content columns joined by '|'.

    Uses Spark's native sha2() — computed in the JVM, no Python UDF overhead.
    The separator '|' is chosen because none of the numeric columns can contain
    it, making collisions impossible for this dataset.
    """
    return df.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws("|", *[F.col(c).cast("string") for c in _CONTENT_COLS]),
            256,
        ),
    )


# ── Merkle tree ────────────────────────────────────────────────────────────────


def compute_merkle_root(hashes: list[str]) -> str:
    """Build a Merkle tree from hex-digest leaves and return the root digest.

    Leaves are sorted before tree construction so the root is deterministic
    regardless of Spark's partition order.  Odd-length layers duplicate the
    last node (standard Bitcoin / RFC 6962 convention).
    """
    if not hashes:
        return ""
    layer = sorted(hashes)
    while len(layer) > 1:
        if len(layer) % 2 == 1:
            layer.append(layer[-1])
        layer = [
            hashlib.sha256((layer[i] + layer[i + 1]).encode()).hexdigest()
            for i in range(0, len(layer), 2)
        ]
    return layer[0]


# ── Manifest store ─────────────────────────────────────────────────────────────


def write_manifest(
    spark: SparkSession,
    layer: str,
    row_count: int,
    merkle_root: str,
    batch_id: str | None = None,
) -> str:
    """Append one manifest record to the _audit/manifests Delta table.

    Returns the batch_id so callers can store it for later verification.
    """
    bid = batch_id or str(uuid.uuid4())
    now = datetime.now(tz=UTC).replace(tzinfo=None)
    row = [(bid, layer, row_count, merkle_root, now)]
    (
        spark.createDataFrame(row, schema=_MANIFEST_SCHEMA)
        .write.format("delta")
        .mode("append")
        .save(AUDIT_PATH)
    )
    logger.info(
        "Manifest written — layer=%s batch=%s rows=%d root=%s…",
        layer,
        bid,
        row_count,
        merkle_root[:16],
    )
    return bid


def latest_manifest(spark: SparkSession, layer: str) -> dict | None:
    """Return the most recently recorded manifest for a given layer, or None."""
    try:
        row = (
            spark.read.format("delta")
            .load(AUDIT_PATH)
            .filter(F.col("layer") == layer)
            .orderBy(F.col("recorded_at").desc())
            .first()
        )
        return row.asDict() if row else None
    except Exception:
        return None


# ── Integrity verification ─────────────────────────────────────────────────────


def verify_bronze_integrity(spark: SparkSession) -> dict:
    """Re-read the live Bronze table and verify its integrity two ways.

    Check 1 — per-row hash:
        Recompute SHA-256 from the content columns for every row and compare
        against the stored row_hash.  Any mismatch means a row was modified
        after ingestion (e.g. direct Parquet file edit).

    Check 2 — Merkle root:
        Rebuild the Merkle root from current row_hash values and compare against
        the root recorded in the audit manifest at ingest time.  A mismatch here
        catches deletions or insertions that check 1 alone would miss.

    Returns a result dict with:
        tampered_row_count  int   — rows where stored hash ≠ recomputed hash
        merkle_match        bool  — True if Merkle root matches manifest
        stored_root         str   — root recorded at ingest time (truncated)
        computed_root       str   — root computed right now (truncated)
        clean               bool  — True only when both checks pass
    """
    df = spark.read.format("delta").load(BRONZE_PATH)

    # Check 1: per-row hash comparison (pure Spark, no driver collect needed)
    tampered = (
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

    # Check 2: rebuild Merkle root from current hashes (driver-side, ~18 MB for 284K rows)
    current_hashes = [r.row_hash for r in df.select("row_hash").collect()]
    computed_root = compute_merkle_root(current_hashes)

    manifest = latest_manifest(spark, "bronze")
    stored_root = manifest["merkle_root"] if manifest else None
    merkle_match = stored_root is not None and computed_root == stored_root

    clean = tampered == 0 and merkle_match
    logger.info(
        "Integrity check — tampered_rows=%d merkle_match=%s clean=%s",
        tampered,
        merkle_match,
        clean,
    )
    return {
        "tampered_row_count": tampered,
        "merkle_match": merkle_match,
        "stored_root": (stored_root or "")[:24] + "…",
        "computed_root": computed_root[:24] + "…",
        "clean": clean,
    }
