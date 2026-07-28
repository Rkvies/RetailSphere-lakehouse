"""
dags/retail_lakehouse_dag.py

Orchestrates the full Bronze -> Silver -> Gold pipeline for the
Enterprise Retail Lakehouse Platform.

Task structure is generated DYNAMICALLY from table_config.yaml (via
list_configured_tables()) rather than hardcoded per table - this DAG
file should never need to change when a new domain is onboarded; only
table_config.yaml does. This is the orchestration-layer instance of the
same config-driven principle applied throughout Modules 2, 7, 8, and 9.

Dependency structure:
    bronze_<table> (parallel, all tables)
        -> silver_<table> (parallel, respects fact vs dimension routing)
            -> gold_fact_sales (waits for ALL silver tasks it depends on -
               specifically dim_customer, dim_product, dim_store, sales -
               not just "the previous task in the list")
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.config_loader import list_configured_tables, load_table_config
from src.common.logger import get_logger, log_pipeline_event
from src.ingestion.bronze_loader import run_bronze_ingestion
from src.transformation.silver_processor import process_fact_table
from src.transformation.scd2_handler import process_dimension_table
from src.aggregation.gold_builder import build_fact_sales_gold

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    # Modest, fixed Airflow-level retry delay - deliberately separate
    # from our @retry decorator (Module 4)'s exponential backoff, which
    # handles TRANSIENT failures INSIDE a single task attempt (e.g. a
    # flaky file read). Airflow's retry here is the outer safety net for
    # failures that survived that inner retry entirely (e.g. the whole
    # task process crashed) - two layers of retry, two different scopes,
    # deliberately not the same mechanism reused twice.
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # would be True + configured SMTP in prod
}

dag = DAG(
    dag_id="retail_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze -> Silver -> Gold pipeline for the Enterprise Retail Lakehouse",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,  # explicit choice - discussed below
    tags=["retail", "lakehouse", "medallion"],
)


def _silver_task_callable(table_name: str):
    """
    Routes to the correct Silver processing function based on the
    table's configured scd_type - keeps the DAG definition itself
    free of if/else branching on table type; the routing decision
    lives once, here, matching the same guard-clause pattern used
    inside process_fact_table/process_dimension_table themselves
    (Modules 8 and 9).
    """
    table_conf = load_table_config(table_name)
    if table_conf.get("scd_type") == 2:
        return process_dimension_table(table_name)
    return process_fact_table(table_name)


bronze_tasks = {}
silver_tasks = {}

for table_name in list_configured_tables():
    bronze_task = PythonOperator(
        task_id=f"bronze_{table_name}",
        python_callable=run_bronze_ingestion,
        op_kwargs={"table_name": table_name},
        dag=dag,
    )
    bronze_tasks[table_name] = bronze_task

    silver_task = PythonOperator(
        task_id=f"silver_{table_name}",
        python_callable=_silver_task_callable,
        op_kwargs={"table_name": table_name},
        dag=dag,
    )
    silver_tasks[table_name] = silver_task

    # Each table's own Bronze must complete before its own Silver runs -
    # this per-table edge is the SAME regardless of table type, so it's
    # expressed once, generically, here.
    bronze_task >> silver_task

gold_fact_sales_task = PythonOperator(
    task_id="gold_fact_sales",
    python_callable=build_fact_sales_gold,
    dag=dag,
)

# Gold's REAL dependency set, encoded explicitly - this is the
# architecture decision from above made concrete: Gold waits for every
# Silver table it actually reads (sales, customer, product, store),
# not just "whatever Silver task happens to be last in iteration order."
GOLD_FACT_SALES_DEPENDENCIES = ["sales", "customer", "product", "store"]

for dependency_table in GOLD_FACT_SALES_DEPENDENCIES:
    silver_tasks[dependency_table] >> gold_fact_sales_task