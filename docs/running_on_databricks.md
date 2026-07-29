# Running on Databricks — Complete Guide

## Step 0 — Prerequisites

- Repo already synced via Databricks Repos
- A cluster attached, recent LTS runtime (15.4 LTS or similar) — Delta is pre-installed
- Unity Catalog available (check for the **Catalog** icon in the left sidebar)

## Catalog Structure (read this before running anything)

Everything lives under one catalog, **`retail_lakehouse`**, split into **one schema per medallion layer** — not one shared schema with prefixed table names. This is the idiomatic Unity Catalog pattern: Catalog Explorer browses by schema, so `gold.fact_sales` groups naturally with every other Gold table.

```
retail_lakehouse (catalog)
├── landing    (schema — holds ONE Volume, "files", for raw CSVs only)
│   └── Volume: files/  →  /Volumes/retail_lakehouse/landing/files/<domain>/*.csv
├── bronze     (schema — managed tables, one per source domain)
│   └── sales, customer, product, store, supplier, inventory,
│       promotions, online_orders, shipping, returns
├── silver     (schema — managed tables, cleansed/conformed/historized)
│   └── same table names as bronze
├── gold       (schema — managed tables, the star schema — THIS is what Power BI connects to)
│   ├── dim_customer, dim_product, dim_store, dim_supplier, dim_date
│   └── fact_sales, fact_returns, fact_online_orders, fact_shipping, fact_inventory_snapshot
└── quarantine (schema — managed tables, rows that failed data quality rules)
    └── same table names as bronze/silver, per stage
```

Only `landing` uses a Volume (raw, unstructured files — exactly what Volumes are for). Bronze/Silver/Gold/Quarantine are all **managed tables** (`saveAsTable`) — no Volume, no `LOCATION`, no cloud storage credentials needed; Unity Catalog owns their storage directly.

## Step 1 — One-time Unity Catalog setup

**If you previously ran this project under the old single-schema structure**, clean it up first:
```sql
-- notebooks/00a_cleanup_old_schema.sql
DROP SCHEMA IF EXISTS retail_lakehouse.lakehouse CASCADE;
```

Then create the new structure:
```sql
-- notebooks/00b_setup_unity_catalog.sql
CREATE CATALOG IF NOT EXISTS retail_lakehouse;
USE CATALOG retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE VOLUME IF NOT EXISTS landing.files;

LIST '/Volumes/retail_lakehouse/landing/files';
SHOW SCHEMAS IN retail_lakehouse;
```

**If `CREATE CATALOG` fails with a permission error**: run `SHOW CATALOGS;` / `SHOW SCHEMAS;` to find one you already have write access to, then update `config/databricks_config.yaml`'s `paths.landing_zone` and `unity_catalog.catalog` to match, and create the five schemas + Volume under that catalog instead.

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

If imports fail, run `%pwd` and manually set `repo_root` to the folder that directly contains `src/`, `config/`, etc.

## Step 3 — Generate landing zone data

```python
from src.ingestion.data_generator import main as generate_data
generate_data()
```
Verify: `dbutils.fs.ls("/Volumes/retail_lakehouse/landing/files/sales/")`

## Step 4 — Run Bronze

```python
from src.pipelines.run_bronze_pipeline import main as run_bronze
print(run_bronze())
```
Verify:
```sql
%sql
SHOW TABLES IN retail_lakehouse.bronze;
SELECT * FROM retail_lakehouse.bronze.sales LIMIT 10;
```

## Step 5 — Run Silver

```python
from src.pipelines.run_silver_pipeline import main as run_silver
print(run_silver())
```
Verify:
```sql
%sql
SHOW TABLES IN retail_lakehouse.silver;
SELECT * FROM retail_lakehouse.silver.customer LIMIT 10;
```
Confirm `customer_sk`, `is_current`, `effective_start_date` are populated.

## Step 6 — Run Gold (builds the FULL star schema — 5 dimensions + 5 facts)

```python
from src.pipelines.run_gold_pipeline import main as run_gold
print(run_gold())
```
This runs `build_all_gold_tables()`, which builds, in dependency order:
1. `dim_customer`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`
2. `fact_sales` (point-in-time joined to the dims above)
3. `fact_returns` (inherits customer_sk/product_sk from fact_sales via original_invoice_id)
4. `fact_online_orders` (point-in-time joined to dim_customer/dim_product)
5. `fact_shipping` (inherits customer_sk from fact_online_orders via order_id)
6. `fact_inventory_snapshot` (point-in-time joined to dim_product, current-value joined to dim_store)

Verify everything built:
```sql
%sql
SHOW TABLES IN retail_lakehouse.gold;
```
Expect: `dim_customer`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`, `fact_sales`, `fact_returns`, `fact_online_orders`, `fact_shipping`, `fact_inventory_snapshot` — 10 tables.

Spot-check the point-in-time join worked:
```sql
%sql
SELECT c.country, SUM(f.line_total) AS total_revenue
FROM retail_lakehouse.gold.fact_sales f
JOIN retail_lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.country
ORDER BY total_revenue DESC;
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

display(spark.sql("SHOW TABLES IN retail_lakehouse.gold"))
```

## Connecting Power BI to the Gold Schema

1. In Power BI Desktop: **Get Data** → **Azure Databricks** (or the generic **Databricks** connector)
2. Server hostname + HTTP path: from your cluster's **Advanced Options → JDBC/ODBC** tab
3. Authentication: Personal Access Token (same kind used for the Databricks CLI)
4. Once connected, navigate to **`retail_lakehouse` → `gold`** — you'll see all 10 tables
5. Import `dim_customer`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`, and the 5 fact tables
6. **Build relationships in Power BI's Model view**:
   - `fact_sales.customer_sk` → `dim_customer.customer_sk`
   - `fact_sales.product_sk` → `dim_product.product_sk`
   - `fact_sales.store_sk` (or `store_id`) → `dim_store`
   - `fact_sales.date_sk` → `dim_date.date_sk`
   - Same pattern for `fact_returns`, `fact_online_orders`, `fact_shipping`, `fact_inventory_snapshot`
7. Set each dimension table's cardinality to **one-to-many** (1 dim row : many fact rows) — standard star schema modeling

**Note on `dim_customer`/`dim_product` in Power BI**: these carry full SCD2 history (`is_current`, `effective_start_date`, `effective_end_date`). For most dashboards you'll want to relate facts using the surrogate key (already point-in-time correct from Gold's join logic) but may want to filter dimension-only visuals (e.g. "current customer list") to `is_current = true`.

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

Fall back to Workspace Files and disable UC entirely:
```yaml
# config/databricks_config.yaml
unity_catalog:
  enabled: false
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
Everything falls back to path-based Delta tables automatically — no other code changes needed (`resolve_table_ref()` handles this branching internally). Power BI would then connect via a Databricks cluster/SQL warehouse query against those paths rather than browsing a catalog schema.

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'src'` | Repo root not on `sys.path` | Re-run Step 2 |
| `Missing cloud file system scheme` | Old code/config trying to register a Volume path as a table `LOCATION` | Make sure you're on the current `gold_builder.py`/`config_loader.py` — Bronze/Silver/Gold are managed tables now, not path-based |
| `AnalysisException: Schema 'bronze' does not exist` | Step 1 setup script wasn't run, or ran against a different catalog | Re-run `00b_setup_unity_catalog.sql`, confirm `unity_catalog.catalog` in config matches |
| Silver/Gold shows 0 rows despite Bronze succeeding | Mixed environment state across cells | Clear State, re-run from Step 2 |
| `fact_returns`/`fact_shipping` show 0 rows | Ran Gold tables out of order manually | Use `run_gold_pipeline.main()` / `build_all_gold_tables()`, which enforces the correct dependency order, rather than calling individual `build_*` functions out of sequence |
| `PicklingError` after repeated edits | Stale cluster state | Detach/reattach notebook to cluster |

## Orchestration

See [`databricks_job_scheduling_guide.md`](databricks_job_scheduling_guide.md) for turning this into a scheduled Databricks Job.
