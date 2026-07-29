"""
tests/unit/test_dag_integrity.py
Structure/wiring tests only - business logic is already covered by
Modules 7-10's own unit tests.
"""
import pytest


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder="dags/", include_examples=False)


def test_dag_loads_without_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"


def test_dag_is_registered(dagbag):
    dag = dagbag.get_dag(dag_id="retail_lakehouse_pipeline")
    assert dag is not None


def test_every_table_has_bronze_and_silver_tasks(dagbag):
    from src.common.config_loader import list_configured_tables
    dag = dagbag.get_dag(dag_id="retail_lakehouse_pipeline")
    task_ids = {task.task_id for task in dag.tasks}
    for table_name in list_configured_tables():
        assert f"bronze_{table_name}" in task_ids
        assert f"silver_{table_name}" in task_ids


def test_gold_fact_sales_depends_on_all_required_silver_tasks(dagbag):
    dag = dagbag.get_dag(dag_id="retail_lakehouse_pipeline")
    gold_task = dag.get_task("gold_fact_sales")
    upstream_task_ids = {t.task_id for t in gold_task.upstream_list}
    for required_table in ["sales", "customer", "product", "store"]:
        assert f"silver_{required_table}" in upstream_task_ids
