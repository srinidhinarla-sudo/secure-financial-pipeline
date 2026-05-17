"""DAG: bronze_ingest — raw CSV → Bronze Delta table."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    dag_id="bronze_ingest",
    description="Ingest creditcard.csv into the Bronze Delta Lake layer",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "ingest", "delta"],
    doc_md="""
## bronze_ingest

Reads the raw Kaggle Credit Card Fraud Detection CSV (284,807 rows) and
writes it to the Bronze Delta Lake table with schema enforcement,
a stable `row_key` for idempotent re-runs, and partition by `ingest_date`.
""",
) as dag:

    def _run_bronze(**context):
        from src.transformations.bronze import run_bronze
        from src.utils.spark_session import get_spark

        spark = get_spark(app_name="BronzeIngest")
        try:
            count = run_bronze(spark)
            context["ti"].xcom_push(key="bronze_row_count", value=count)
        finally:
            spark.stop()

    ingest_task = PythonOperator(
        task_id="ingest_csv_to_bronze",
        python_callable=_run_bronze,
        doc_md="Read raw CSV and write to Bronze Delta table via MERGE.",
    )
