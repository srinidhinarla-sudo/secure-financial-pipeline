"""DAG: gold_aggregate — Silver → Gold Delta tables (summaries + fraud signals)."""

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
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(minutes=15),
    "on_failure_callback": send_failure_alert,
}

with DAG(
    dag_id="gold_aggregate",
    description="Aggregate Silver data into daily and hourly Gold Delta summaries",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold", "aggregate", "delta"],
    doc_md="""
## gold_aggregate

Reads from Silver and produces two Gold tables:
- **daily_summary**: total transactions, fraud count, fraud rate, total volume,
  avg/max amount per `transaction_date`
- **hourly_summary**: same metrics broken out by `transaction_hour`, with the
  dominant `amount_bucket` computed via a broadcast join

Both tables use Delta MERGE so re-runs are idempotent.
""",
) as dag:

    def _silver_table_ready():
        import os

        delta_dir = os.getenv("PIPELINE_DELTA_DIR", "/opt/airflow/data/delta")
        return os.path.isdir(os.path.join(delta_dir, "silver", "transactions", "_delta_log"))

    wait_for_silver = PythonSensor(
        task_id="wait_for_silver_clean",
        python_callable=_silver_table_ready,
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        doc_md="Block until the Silver Delta table exists on disk.",
    )

    def _run_gold(**context):
        from src.transformations.gold import run_gold
        from src.utils.spark_session import get_spark

        spark = get_spark(app_name="GoldAggregate")
        try:
            counts = run_gold(spark)
            context["ti"].xcom_push(key="gold_row_counts", value=counts)
        finally:
            spark.stop()

    aggregate_task = PythonOperator(
        task_id="aggregate_silver_to_gold",
        python_callable=_run_gold,
        doc_md="Build daily and hourly Gold aggregations with broadcast joins and Delta MERGE.",
    )

    def _run_anomaly(**context):
        from src.transformations.anomaly import run_anomaly_detection
        from src.utils.spark_session import get_spark

        spark = get_spark(app_name="AnomalyDetection")
        try:
            metrics = run_anomaly_detection(spark)
            context["ti"].xcom_push(key="anomaly_metrics", value=metrics)
        finally:
            spark.stop()

    anomaly_task = PythonOperator(
        task_id="fraud_anomaly_detection",
        python_callable=_run_anomaly,
        doc_md=(
            "Train Isolation Forest on V1-V28 + Amount (unsupervised). "
            "Write fraud_signals Gold table. Push precision/recall/F1 to XCom."
        ),
    )

    wait_for_silver >> aggregate_task >> anomaly_task
