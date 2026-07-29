# Databricks notebook source
# MAGIC %md
# MAGIC ## Register existing Delta writes as Unity Catalog tables
# MAGIC
# MAGIC Run this ONCE if you already ran the pipeline before this fix landed -
# MAGIC it registers your EXISTING Bronze/Silver/Gold Delta files as proper
# MAGIC catalog tables without re-running ingestion/transformation. New runs
# MAGIC after this fix register automatically - this script is only needed
# MAGIC to "catch up" data you already produced.

# COMMAND ----------

import sys, os
repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.common.config_loader import (
    list_configured_tables, resolve_layer_path, resolve_catalog_table_name,
)
from src.common.delta_utils import register_as_table, _table_exists

# COMMAND ----------

registered = []
skipped = []

for table_name in list_configured_tables():
    for layer in ["bronze", "silver"]:
        path = resolve_layer_path(layer, table_name)
        catalog_name = resolve_catalog_table_name(layer, table_name)
        if _table_exists(spark, path):
            register_as_table(spark, path, catalog_name)
            registered.append(catalog_name)
        else:
            skipped.append(f"{layer}_{table_name} (no Delta table found at {path})")

# Gold fact_sales specifically (only Gold table this project currently builds)
gold_path = resolve_layer_path("gold", "fact_sales")
if _table_exists(spark, gold_path):
    register_as_table(spark, gold_path, resolve_catalog_table_name("gold", "fact_sales"))
    registered.append(resolve_catalog_table_name("gold", "fact_sales"))
else:
    skipped.append("gold_fact_sales (not yet built)")

print(f"Registered {len(registered)} tables:")
for r in registered:
    print(f"  - {r}")

print(f"\nSkipped {len(skipped)} (no existing Delta data found):")
for s in skipped:
    print(f"  - {s}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_lakehouse.lakehouse;
