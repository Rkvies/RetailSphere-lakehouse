"""
tests/unit/test_dag_integrity.py

DAG integrity tests - these validate STRUCTURE (no import errors, no
cycles, correct dependency edges), not business logic. The actual
pipeline logic (bronze_loader, silver_processor, etc.) is already
covered by its own unit tests in Modules 7-10 - re-testing that logic
here would be redundant and would make this test fragile for the
wrong reasons.
"""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
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
        assert f"silver_{required_table}" in upstream_task_ids, (
            f"gold_fact_sales is missing dependency on silver_{required_table} - "
            f"this would let Gold run against incomplete Silver data."
        )


def test_dag_has_no_cycles(dagbag):
    dag = dagbag.get_dag(dag_id="retail_lakehouse_pipeline")
    # DagBag.process_file (called internally during DagBag construction)
    # already raises on cycles - if we got a valid dag object at all,
    # this assertion documents the property being relied upon, making
    # the guarantee explicit rather than implicit in "the fixture didn't crash."
    assert dag is not None