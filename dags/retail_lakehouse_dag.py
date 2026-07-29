"""
dags/retail_lakehouse_dag.py
Orchestrates Bronze -> Silver -> Gold. Tasks are generated dynamically
from table_config.yaml - this file never needs to change when a new
domain is onboarded.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.config_loader import list_configured_tables, load_table_config
from src.common.logger import get_logger
from src.ingestion.bronze_loader import run_bronze_ingestion
from src.transformation.silver_processor import process_fact_table
from src.transformation.scd2_handler import process_dimension_table
from src.aggregation.gold_builder import build_fact_sales_gold

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="retail_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze -> Silver -> Gold pipeline for the Enterprise Retail Lakehouse",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["retail", "lakehouse", "medallion"],
)


def _silver_task_callable(table_name: str):
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

    bronze_task >> silver_task

gold_fact_sales_task = PythonOperator(
    task_id="gold_fact_sales",
    python_callable=build_fact_sales_gold,
    dag=dag,
)

GOLD_FACT_SALES_DEPENDENCIES = ["sales", "customer", "product", "store"]
for dependency_table in GOLD_FACT_SALES_DEPENDENCIES:
    if dependency_table in silver_tasks:
        silver_tasks[dependency_table] >> gold_fact_sales_task
