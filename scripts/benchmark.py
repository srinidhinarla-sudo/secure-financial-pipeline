"""Benchmark optimized vs unoptimized silver-layer performance.

Runs the Silver transformation twice on the same Bronze data and reports
wall-clock times.  Wipes the Silver table between runs so each pass starts
from the same initial-load code path (no MERGE shortcut).

Usage:
    python scripts/benchmark.py
"""

from __future__ import annotations

import os
import shutil
import time

os.environ.setdefault("PIPELINE_DATA_DIR", os.path.abspath("data"))
os.environ.setdefault("PIPELINE_DELTA_DIR", os.path.abspath("data/delta"))
os.environ.setdefault("SLACK_WEBHOOK_URL", "")

from src.config import BRONZE_PATH, SILVER_PATH  # noqa: E402
from src.transformations.silver import run_silver  # noqa: E402
from src.utils.spark_session import get_spark, stop_spark  # noqa: E402


def _wipe_silver() -> None:
    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)


def _run(label: str, optimized: bool) -> float:
    print(f"\n  [{label}] starting…")
    _wipe_silver()
    spark = get_spark(app_name=f"Benchmark_{label}", optimized=optimized)
    t0 = time.time()
    count = run_silver(spark, optimized=optimized)
    elapsed = time.time() - t0
    stop_spark(spark)
    m, s = divmod(int(elapsed), 60)
    print(f"  [{label}] {count:,} rows  →  {m}m {s}s  ({elapsed:.1f}s)")
    return elapsed


def main() -> None:
    print("=" * 58)
    print("  Silver-layer benchmark  (optimized vs unoptimized)")
    print("  Source: Bronze Delta table at", BRONZE_PATH)
    print("=" * 58)

    t_opt = _run("optimized  (AQE + cache)", optimized=True)
    t_base = _run("unoptimized (no AQE, no cache)", optimized=False)

    speedup = t_base / t_opt if t_opt > 0 else 0
    print("\n" + "=" * 58)
    print(f"  Optimized:   {t_opt:.1f}s")
    print(f"  Unoptimized: {t_base:.1f}s")
    print(f"  Speedup:     {speedup:.2f}×")
    print("=" * 58)

    _wipe_silver()
    print("\n  Silver table wiped — run scripts/run_pipeline.py to rebuild.")


if __name__ == "__main__":
    main()
