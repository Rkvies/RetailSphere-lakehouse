# Running on Databricks — Complete Guide

## Step 0 — Prerequisites

- Repo already synced via Databricks Repos
- A cluster attached, recent LTS runtime (15.4 LTS or similar) — Delta is pre-installed
- Unity Catalog available (check for the **Catalog** icon in the left sidebar). If missing, see the "No Unity Catalog" fallback near the end.

## Step 1 — One-time Unity Catalog setup

Run once, in a SQL cell (`%sql`) or the SQL editor:

```sql
SHOW CATALOGS;

CREATE CATALOG IF NOT EXISTS retail_lakehouse;
USE CATALOG retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS lakehouse;
USE SCHEMA lakehouse;

CREATE VOLUME IF NOT EXISTS landing;
CREATE VOLUME IF NOT EXISTS lakehouse_data;

LIST '/Volumes/retail_lakehouse/lakehouse/landing';
```

**If `CREATE CATALOG` fails with a permission error**: run `SHOW CATALOGS;` / `SHOW SCHEMAS;` to find one you already have write access to, then update `config/databricks_config.yaml`'s five `paths.*` values to point there, and create the two volumes under that schema instead.

## Step 2 — First cell of your run notebook

```python
import sys, os

repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

os.environ["ENV"] = "databricks"

print("Repo root:", repo_root)
print("ENV:", os.environ["ENV"])
```

If imports still fail, run `%pwd` and manually set `repo_root` to the folder that directly contains `src/`, `config/`, etc.

## Step 3 — Generate landing zone data

```python
from src.ingestion.data_generator import main as generate_data
generate_data()
```
Verify: `dbutils.fs.ls("/Volumes/retail_lakehouse/lakehouse/landing/sales/")`

## Step 4 — Run Bronze

```python
from src.pipelines.run_bronze_pipeline import main as run_bronze
print(run_bronze())
```
Verify — Bronze is now a real managed catalog table, query it directly by name:
```python
display(spark.table("retail_lakehouse.lakehouse.bronze_sales"))
```
```sql
%sql
SELECT * FROM retail_lakehouse.lakehouse.bronze_sales LIMIT 10;
```

## Step 5 — Run Silver

```python
from src.pipelines.run_silver_pipeline import main as run_silver
print(run_silver())
```
Verify:
```python
display(spark.table("retail_lakehouse.lakehouse.silver_customer"))
```
Confirm `customer_sk`, `is_current`, `effective_start_date` are populated.

## Step 6 — Run Gold

```python
from src.pipelines.run_gold_pipeline import main as run_gold
print(run_gold())
```
Verify (the real payoff — proves the point-in-time join worked):
```python
display(
    spark.table("retail_lakehouse.lakehouse.gold_fact_sales")
    .groupBy("customer_sk").sum("line_total")
    .orderBy("sum(line_total)", ascending=False)
    .limit(10)
)
```

**Every Bronze/Silver/Gold table is a real, managed Unity Catalog table** — not a raw Delta path you have to know — because `merge_upsert`/`scd2_merge`/writes now target `catalog.schema.table` names directly via `saveAsTable`, not a Volume path with a separate registration step:
```sql
%sql
SHOW TABLES IN retail_lakehouse.lakehouse;
```

## Full One-Cell Run (quick end-to-end demo)

```python
import sys, os
repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.ingestion.data_generator import main as generate_data
from src.pipelines.run_bronze_pipeline import main as run_bronze
from src.pipelines.run_silver_pipeline import main as run_silver
from src.pipelines.run_gold_pipeline import main as run_gold

print("Generating data..."); generate_data()
print("Bronze:", run_bronze())
print("Silver:", run_silver())
print("Gold:", run_gold())

display(
    spark.table("retail_lakehouse.lakehouse.gold_fact_sales")
    .groupBy("customer_sk").sum("line_total")
    .orderBy("sum(line_total)", ascending=False)
    .limit(10)
)
```

## Running Tests on Databricks

```python
%pip install pytest pytest-cov
```
```python
import subprocess
result = subprocess.run(
    ["pytest", f"{repo_root}/tests/unit/", "-v", "--tb=short"],
    capture_output=True, text=True, cwd=repo_root,
)
print(result.stdout[-4000:])
```

## No Unity Catalog available?

Fall back to Workspace Files:
```yaml
# config/databricks_config.yaml
paths:
  landing_zone: "/Workspace/Shared/retail_lakehouse/landing"
  bronze: "/Workspace/Shared/retail_lakehouse/bronze"
  silver: "/Workspace/Shared/retail_lakehouse/silver"
  gold: "/Workspace/Shared/retail_lakehouse/gold"
  quarantine: "/Workspace/Shared/retail_lakehouse/quarantine"
```
```python
dbutils.fs.mkdirs("file:/Workspace/Shared/retail_lakehouse/landing")
```

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'src'` | Repo root not on `sys.path` | Re-run Step 2 |
| `AnalysisException: Path does not exist` | `ENV` not set before imports, or catalog/schema/volume not created | Re-run Step 1, confirm Step 2's `os.environ["ENV"]` ran first |
| Silver/Gold shows 0 rows despite Bronze succeeding | Mixed environment state across cells | Clear State, re-run from Step 2 |
| `PicklingError` after repeated edits | Stale cluster state | Detach/reattach notebook to cluster |

## Orchestration

See [`databricks_job_scheduling_guide.md`](databricks_job_scheduling_guide.md) for turning this into a scheduled Databricks Job.
