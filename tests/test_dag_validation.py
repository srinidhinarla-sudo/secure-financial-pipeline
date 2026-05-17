"""DAG structure tests — validate DAGs load without errors and have correct config."""

import os
from datetime import timedelta
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
DAG_FILES = ["bronze_ingest", "silver_clean", "gold_aggregate"]


@pytest.fixture(scope="module", autouse=True)
def airflow_home(tmp_path_factory):
    """Point Airflow at a temp dir so tests don't need a running metadata DB."""
    tmp = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(tmp)
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{tmp}/airflow.db"
    # Prevent Airflow from loading built-in example DAGs
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
    yield
    for key in (
        "AIRFLOW_HOME",
        "AIRFLOW__CORE__EXECUTOR",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__CORE__LOAD_EXAMPLES",
    ):
        os.environ.pop(key, None)


def _load_dag(module_name: str):
    """Import a DAG module and return its DagBag."""
    from airflow.models import DagBag

    dag_path = str(DAGS_DIR / f"{module_name}.py")
    bag = DagBag(dag_folder=dag_path, include_examples=False)
    return bag


# ── test 16 ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_loads_without_import_errors(dag_module):
    """Each DAG file must import cleanly — zero import errors."""
    bag = _load_dag(dag_module)
    assert bag.import_errors == {}, f"Import errors in {dag_module}: {bag.import_errors}"


# ── test 17 ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_has_expected_id(dag_module):
    """The dag_id inside each file must match the filename."""
    bag = _load_dag(dag_module)
    assert dag_module in bag.dags, f"dag_id '{dag_module}' not found in DagBag"


# ── test 18 ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_default_args_retries(dag_module):
    """All DAGs must specify retries=3 in default_args."""
    bag = _load_dag(dag_module)
    dag = bag.dags[dag_module]
    default_args = dag.default_args
    assert (
        default_args.get("retries") == 3
    ), f"{dag_module}: expected retries=3, got {default_args.get('retries')}"


# ── test 19 ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_default_args_sla(dag_module):
    """All DAGs must carry sla=timedelta(minutes=15) in default_args."""
    bag = _load_dag(dag_module)
    dag = bag.dags[dag_module]
    sla = dag.default_args.get("sla")
    assert sla == timedelta(minutes=15), f"{dag_module}: expected sla=15min, got {sla}"


# ── test 20 ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_has_on_failure_callback(dag_module):
    """All DAGs must wire send_failure_alert as on_failure_callback."""
    bag = _load_dag(dag_module)
    dag = bag.dags[dag_module]
    callback = dag.default_args.get("on_failure_callback")
    assert callback is not None, f"{dag_module}: on_failure_callback not set"
    assert (
        callback.__name__ == "send_failure_alert"
    ), f"{dag_module}: unexpected callback {callback}"
