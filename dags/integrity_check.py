"""DAG: integrity_check — cryptographic audit of the Bronze Delta table.

Runs daily after the Gold aggregation.  Re-reads the live Bronze table,
recomputes SHA-256 hashes for every row, and verifies them two ways:

  1. Per-row: stored row_hash == SHA-256(content columns) for each row.
  2. Merkle root: Merkle tree rebuilt from current hashes == root recorded
     at ingest time in the _audit/manifests table.

Any mismatch triggers a Slack alert identical in format to pipeline failure
alerts — making data-integrity violations visible to the same on-call channel
as task failures.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from src.utils.slack_alerts import send_failure_alert

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=15),
    "on_failure_callback": send_failure_alert,
}

with DAG(
    dag_id="integrity_check",
    description="Cryptographic tamper-detection audit of the Bronze Delta table",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["security", "audit", "integrity"],
    doc_md="""
## integrity_check

Re-reads the Bronze Delta table, recomputes SHA-256 row hashes from the raw
financial columns, and compares them against the hashes stored at ingest time.
Also rebuilds the Merkle root and checks it against the manifest recorded by
the bronze_ingest DAG.

**Detects:**
- In-place modification of Parquet files (per-row hash mismatch)
- Row deletions or insertions not captured by row-level checks (Merkle root mismatch)

**Alert:** fires the same Slack webhook as pipeline failures if tampering is found.
""",
) as dag:

    def _audit_table_ready():
        import os

        delta_dir = os.getenv("PIPELINE_DELTA_DIR", "/opt/airflow/data/delta")
        return os.path.isdir(os.path.join(delta_dir, "_audit", "manifests", "_delta_log"))

    wait_for_audit_manifest = PythonSensor(
        task_id="wait_for_audit_manifest",
        python_callable=_audit_table_ready,
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        doc_md="Block until the first manifest has been written by bronze_ingest.",
    )

    def _run_integrity_check(**context):
        from src.security.audit import verify_bronze_integrity
        from src.utils.slack_alerts import send_failure_alert
        from src.utils.spark_session import get_spark

        spark = get_spark(app_name="IntegrityCheck")
        try:
            result = verify_bronze_integrity(spark)
            context["ti"].xcom_push(key="integrity_result", value=result)

            if not result["clean"]:
                problems = []
                if result["tampered_row_count"] > 0:
                    problems.append(f"{result['tampered_row_count']} rows have mismatched hashes")
                if not result["merkle_match"]:
                    problems.append(
                        f"Merkle root mismatch "
                        f"(stored={result['stored_root']} "
                        f"computed={result['computed_root']})"
                    )

                alert_context = {
                    "dag": context["dag"],
                    "task_instance": context["ti"],
                    "exception": Exception(
                        "INTEGRITY VIOLATION detected in Bronze table: " + "; ".join(problems)
                    ),
                    "logical_date": context.get("logical_date"),
                }
                send_failure_alert(alert_context)
                raise ValueError("Bronze table integrity check FAILED: " + "; ".join(problems))

        finally:
            spark.stop()

    check_task = PythonOperator(
        task_id="verify_bronze_integrity",
        python_callable=_run_integrity_check,
        doc_md=(
            "Recompute SHA-256 hashes and Merkle root for the Bronze table. "
            "Raises on any tamper evidence."
        ),
    )

    wait_for_audit_manifest >> check_task
