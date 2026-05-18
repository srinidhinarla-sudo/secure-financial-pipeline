"""Simulate a file-level tamper attack on the Bronze Delta table.

Models the threat: an attacker with filesystem access (e.g. a compromised
storage node or insider threat) directly edits a Parquet file to alter
transaction records — bypassing Delta Lake's transaction log entirely.

The script:
  1. Picks a Parquet file from the Bronze table.
  2. Reads it with pandas (raw file access, no Delta Lake).
  3. Flips the Class label and inflates the Amount on the first row
     to simulate fraud record manipulation.
  4. Writes the file back, leaving the Delta transaction log untouched
     (so Delta itself would still "see" the table as valid).
  5. Runs verify_bronze_integrity() and shows that the Merkle-root
     audit catches both the per-row hash mismatch and the root deviation.

Usage:
    python scripts/simulate_tamper.py
"""

from __future__ import annotations

import glob
import os
import sys

os.environ.setdefault("PIPELINE_DATA_DIR", os.path.abspath("data"))
os.environ.setdefault("PIPELINE_DELTA_DIR", os.path.abspath("data/delta"))
os.environ.setdefault("SLACK_WEBHOOK_URL", "")

import pandas as pd  # noqa: E402
from src.config import BRONZE_PATH  # noqa: E402
from src.security.audit import verify_bronze_integrity  # noqa: E402
from src.utils.spark_session import get_spark, stop_spark  # noqa: E402

_SEP = "=" * 62


def main() -> None:
    print(f"\n{_SEP}")
    print("  TAMPER SIMULATION — Bronze Delta Table Attack")
    print(_SEP)

    # ── 1. Find a target Parquet file ─────────────────────────────────────────
    parquet_files = sorted(glob.glob(f"{BRONZE_PATH}/**/*.parquet", recursive=True))
    if not parquet_files:
        print("\nERROR: No Parquet files found. Run scripts/run_pipeline.py first.")
        sys.exit(1)

    target = parquet_files[0]
    print("\n[ATTACKER] Target file selected:")
    print(f"           {target}")

    # ── 2. Read → mutate → write back (bypassing Delta Lake) ─────────────────
    # Use pyarrow directly to preserve Spark-written Parquet metadata exactly.
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(target)
    pdf = table.to_pandas()
    original_class = int(pdf["Class"].iloc[0])
    original_amount = float(pdf["Amount"].iloc[0])
    tampered_class = 1 - original_class
    tampered_amount = 99_999.99

    pdf.at[pdf.index[0], "Class"] = tampered_class
    pdf.at[pdf.index[0], "Amount"] = tampered_amount

    # Write back preserving the original Arrow schema so Spark can still read
    # the file — making the tamper harder to detect at the file-format level.
    pq.write_table(pa.Table.from_pandas(pdf, schema=table.schema), target, compression="snappy")

    # Spark writes a .crc sidecar next to every Parquet file for local-fs
    # checksumming.  A realistic attacker removes it so Spark doesn't reject
    # the modified file on next read.  Our audit detects the tamper anyway via
    # SHA-256 row hashes — not the CRC.
    crc_path = os.path.join(os.path.dirname(target), f".{os.path.basename(target)}.crc")
    if os.path.exists(crc_path):
        os.remove(crc_path)

    print("\n[ATTACKER] Row 0 mutated:")
    print(
        f"           Class:  {original_class} → {tampered_class}  "
        f"({'legit→fraud' if tampered_class == 1 else 'fraud→legit'})"
    )
    print(f"           Amount: ${original_amount:>10.2f} → ${tampered_amount:>10.2f}")
    print("           Delta transaction log: UNTOUCHED  ← attacker's blind spot")

    # ── 3. Run integrity check ────────────────────────────────────────────────
    print("\n[DEFENDER] Running integrity_check DAG task…")
    spark = get_spark(app_name="TamperDemo")
    result = verify_bronze_integrity(spark)
    stop_spark(spark)

    # ── 4. Report ─────────────────────────────────────────────────────────────
    print(f"\n{_SEP}")
    print("  INTEGRITY CHECK RESULTS")
    print(_SEP)
    print(f"  Rows with hash mismatch : {result['tampered_row_count']:,}")
    print(f"  Merkle root matches     : {result['merkle_match']}")
    print(f"  Stored root (ingest)    : {result['stored_root']}")
    print(f"  Computed root (now)     : {result['computed_root']}")
    print(f"  Pipeline clean          : {result['clean']}")

    if not result["clean"]:
        print(f"\n{'!' * 62}")
        print("  *** INTEGRITY VIOLATION DETECTED ***")
        print(f"  → {result['tampered_row_count']} row(s) modified after ingestion")
        print("  → Merkle root deviation confirms batch-level tampering")
        print("  → Slack alert fired to #security-alerts")
        print(f"{'!' * 62}")
    else:
        print("\n  No tampering detected (unexpected — check setup).")


if __name__ == "__main__":
    main()
