# Running on Databricks — Step by Step

## Step 0 — Confirm the setup

1. **Repos:** Workspace → Repos → confirm your GitHub repo is checked out (you said this is done)
2. **Cluster:** Create/attach a cluster on a recent LTS runtime (e.g. 15.4 LTS) — Delta Lake is pre-installed, you do **not** need to `pip install delta-spark`
3. **Verify the code is importable:** open (or create) a notebook **inside the repo folder** in Repos — notebooks placed inside the repo can import `src...` modules directly once the repo root is on `sys.path` (Step 1 below)

## Step 1 — First cell of every notebook: path setup

```python
import sys, os

# If this notebook lives inside the repo (recommended), this resolves
# the repo root automatically. Otherwise, replace with the path shown
# under Repos > your repo > "Copy path" in the workspace UI.
repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

print("sys.path includes:", repo_root)
```

**If your `src` modules still don't import after this:** your notebook is probably one level deeper/shallower than expected. Run `%pwd` in a cell and manually set `repo_root` to the folder that directly contains `src/`, `config/`, etc.

## Step 2 — Storage: Unity Catalog Volumes (Community Edition has no DBFS root access)

1. Confirm Unity Catalog is available: check for the **Catalog** icon in the left sidebar. If it's missing, UC isn't enabled on your workspace — skip to the "No Unity Catalog?" fallback at the bottom.
2. Run `notebooks/00_setup_unity_catalog.sql` once (SQL editor or a `%sql` cell) to create the catalog/schema/volumes. **If `CREATE CATALOG` fails with a permissions error**, your CE account likely can't create catalogs — instead find whatever default catalog/schema `SHOW CATALOGS;` / `SHOW SCHEMAS;` already gives you write access to, and update `config/databricks_config.yaml`'s five paths to point under that instead.
3. Set the environment variable at the top of your run notebook:
```python
import os
os.environ["ENV"] = "databricks"
```
This makes `config_loader.py` merge `config/databricks_config.yaml` on top of `config/base_config.yaml` — every path (`landing_zone`, `bronze`, `silver`, `gold`, `quarantine`) now resolves under `/Volumes/...` automatically. **No other file needs manual path edits** — `resolve_layer_path()` in `config_loader.py` is the single place every pipeline module gets its paths from now.

### No Unity Catalog available at all?
Fall back to **Workspace Files** (not DBFS root, not UC — a third option, always available even on restricted CE):
```yaml
# config/databricks_config.yaml, paths section, replacing the /Volumes/... values:
paths:
  landing_zone: "/Workspace/Shared/retail_lakehouse/landing"
  bronze: "/Workspace/Shared/retail_lakehouse/bronze"
  silver: "/Workspace/Shared/retail_lakehouse/silver"
  gold: "/Workspace/Shared/retail_lakehouse/gold"
  quarantine: "/Workspace/Shared/retail_lakehouse/quarantine"
```
Workspace Files have lower size/performance ceilings than Volumes and aren't meant for large-scale data (fine for this portfolio-scale demo). Create the folder first: `dbutils.fs.mkdirs("file:/Workspace/Shared/retail_lakehouse/landing")` or via the Workspace UI.

## Step 3 — Generate landing zone data

```python
os.environ["ENV"] = "databricks"   # must be set before this import resolves paths
from src.ingestion.data_generator import main as generate_data
generate_data()
```
Verify:
```python
dbutils.fs.ls("/Volumes/retail_lakehouse/lakehouse/landing/sales/")
```

## Step 4 — Run Bronze

```python
from src.pipelines.run_bronze_pipeline import main as run_bronze
run_bronze()
```
Verify:
```python
from src.common.config_loader import resolve_layer_path
display(spark.read.format("delta").load(resolve_layer_path("bronze", "sales")))
```

## Step 5 — Run Silver

```python
from src.pipelines.run_silver_pipeline import main as run_silver
run_silver()
```
Verify:
```python
display(spark.read.format("delta").load(resolve_layer_path("silver", "customer")))
```

## Step 6 — Run Gold

```python
from src.pipelines.run_gold_pipeline import main as run_gold
run_gold()
```
Verify:
```python
display(
    spark.read.format("delta").load(resolve_layer_path("gold", "fact_sales"))
    .groupBy("customer_sk").sum("line_total")
    .orderBy("sum(line_total)", ascending=False)
)
```

## Step 7 — Orchestration on Databricks (instead of Airflow)

Airflow isn't available inside Databricks notebooks. Two reasonable options, worth naming both if asked in an interview:

**Option A — Databricks Workflows (recommended, idiomatic):** Workflows → Create Job → add three tasks (Bronze, Silver, Gold) each pointing at the relevant notebook/script, wire Silver to depend on Bronze and Gold to depend on Silver via the UI's task dependency graph. This is Databricks' native equivalent of what our Airflow DAG expressed in code.

**Option B — Keep the Airflow DAG concept, but note it needs separate infrastructure:** the DAG file (`dags/retail_lakehouse_dag.py`) was designed to run in an Airflow environment (local, MWAA, Cloud Composer) that calls out to Databricks Jobs remotely — not inside a Databricks notebook itself. If your goal is "prove Airflow orchestration works" for the portfolio, run it locally per `RUNNING.md`'s Step 9, separate from this Databricks run.

## Common Databricks-Specific Errors

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'src'` | Repo root not on `sys.path` | Re-run Step 1's path-setup cell |
| `AnalysisException: Path does not exist` | Using local-style paths (`data/bronze/...`) instead of DBFS paths | Apply Step 2's config changes |
| Silver/Gold shows 0 rows despite Bronze succeeding | Watermark logic reading from the wrong (local-style) Silver path while Bronze wrote to the DBFS path, or vice versa — a path-mismatch, not a logic bug | Confirm every layer uses the SAME `ENV=databricks` config consistently |
| `PicklingError` or similar from `_deduplicate_within_batch`/UDFs | Rare — usually from stale cluster state after repeated `%run` of changed code | Detach and reattach the notebook to the cluster to clear cached state |
