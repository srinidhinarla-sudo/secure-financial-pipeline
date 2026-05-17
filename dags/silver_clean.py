"""DAG: silver_clean — Bronze → Silver Delta table (clean, enrich, Z-ORDER)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from src.utils.slack_alerts import send_failure_alert

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(minutes=15),
    "on_failure_callback": send_failure_alert,
}

with DAG(
    dag_id="silver_clean",
    description="Deduplicate, enrich, and Z-ORDER Bronze data into the Silver Delta layer",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["silver", "clean", "delta"],
    doc_md="""
## silver_clean

Reads from Bronze, deduplicates on `row_key`, derives `transaction_date`,
`transaction_hour`, `amount_bucket`, `is_fraud`, and `amount_log1p`, then
MERGEs into the Silver Delta table and runs `OPTIMIZE … ZORDER BY transaction_date`.

**Target runtime (optimized mode):** ~3 minutes
**Benchmarking mode:** set `SILVER_UNOPTIMIZED=1` to disable caching/AQE (~6 minutes).
""",
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingest",
        external_dag_id="bronze_ingest",
        external_task_id="ingest_csv_to_bronze",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        doc_md="Block until the Bronze ingest task has succeeded for this execution date.",
    )

    def _run_silver(**context):
        from src.transformations.silver import run_silver
        from src.utils.spark_session import get_spark

        spark = get_spark(app_name="SilverClean")
        try:
            count = run_silver(spark)
            context["ti"].xcom_push(key="silver_row_count", value=count)
        finally:
            spark.stop()

    clean_task = PythonOperator(
        task_id="clean_bronze_to_silver",
        python_callable=_run_silver,
        doc_md="Deduplicate, enrich, MERGE to Silver, then OPTIMIZE ZORDER BY transaction_date.",
    )

    wait_for_bronze >> clean_task
