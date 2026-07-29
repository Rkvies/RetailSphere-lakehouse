# Configuring a Scheduled Databricks Job

Databricks Workflows is the native equivalent of the Airflow DAG for this project — it doesn't run inside a notebook cell, it's configured through the Jobs UI (or the Jobs API/CLI, shown at the end for a code-driven alternative).

## Step 1 — Create three notebooks (thin wrappers around each pipeline stage)

Create these inside your repo (e.g. under `notebooks/jobs/`) so they're version-controlled alongside everything else:

**`notebooks/jobs/01_bronze.py`**
```python
# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.pipelines.run_bronze_pipeline import main as run_bronze
result = run_bronze()
print(result)
```

**`notebooks/jobs/02_silver.py`**
```python
# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.pipelines.run_silver_pipeline import main as run_silver
result = run_silver()
print(result)
```

**`notebooks/jobs/03_gold.py`**
```python
# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.pipelines.run_gold_pipeline import main as run_gold
result = run_gold()
print(result)
```

(Optional) **`notebooks/jobs/00_generate_data.py`** — only needed the first time, or if you want each scheduled run to regenerate fresh synthetic data (unusual for a real pipeline, but useful for a repeatable demo):
```python
# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.ingestion.data_generator import main as generate_data
generate_data()
```

## Step 2 — Create the Job via the UI

1. Left sidebar → **Workflows** → **Create Job**
2. Name it `retail_lakehouse_pipeline`
3. **Task 1**: name `bronze`, type = Notebook, path = `notebooks/jobs/01_bronze.py`, cluster = your existing cluster (or "New Job Cluster" for cost isolation — see note below)
4. **Task 2**: name `silver`, type = Notebook, path = `notebooks/jobs/02_silver.py`, **Depends on** = `bronze`
5. **Task 3**: name `gold`, type = Notebook, path = `notebooks/jobs/03_gold.py`, **Depends on** = `silver`
6. (Optional) **Task 0**: name `generate_data`, path = `notebooks/jobs/00_generate_data.py`, with `bronze` depending on it instead of running standalone — only include this if you want fresh synthetic data every run

This produces the same dependency graph as `dags/retail_lakehouse_dag.py` expresses in code — `bronze → silver → gold`, enforced by the UI rather than Airflow operators.

## Step 3 — Configure the Schedule

1. In the Job's **Schedule** panel → **Add trigger** → **Scheduled**
2. Set cadence — e.g. `Daily at 02:00` (cron: `0 2 * * ?`), matching the `@daily` schedule in the Airflow DAG for consistency
3. Set timezone appropriately

## Step 4 — Configure Retries and Alerts (the production-readiness parity with Airflow's `DEFAULT_ARGS`)

Per-task, under **Advanced options**:
- **Retries**: 2 (matches `dags/retail_lakehouse_dag.py`'s `DEFAULT_ARGS["retries"]`)
- **Retry interval**: 5 minutes (matches `retry_delay`)
- **Timeout**: set a reasonable ceiling (e.g. 30 min per task) so a stuck task doesn't run indefinitely and block the schedule

Under **Job notifications**:
- Add an email or webhook for **On failure** — this is the Databricks-native equivalent of Airflow's `email_on_failure` (which we deliberately left `False` in the DAG, documenting it as a stand-in for production alerting — this is where you'd actually wire it up)

## Step 5 — Cluster Choice: Job Cluster vs. All-Purpose Cluster

| Option | When to use |
|---|---|
| **New Job Cluster** (spins up per run, terminates after) | Recommended for a real schedule — matches the cost-optimization principle from `docs/architecture.md` (compute only during actual processing, not idle between runs) |
| **Existing All-Purpose Cluster** | Fine for iterative development/demo runs where you're actively watching output, but leaves a cluster billing while idle between scheduled runs — avoid for a genuinely scheduled job |

## Step 6 — Run and Verify

1. Click **Run now** to test outside the schedule first
2. Check each task's output in the Job Run UI — confirm `bronze` → `silver` → `gold` all show green
3. Query the Gold table from a separate notebook to confirm the scheduled run actually produced fresh data:
```python
from src.common.config_loader import resolve_layer_path
display(spark.read.format("delta").load(resolve_layer_path("gold", "fact_sales")))
```

## Alternative: Databricks CLI / Jobs API (code-driven, version-controllable)

For a more "infrastructure as code" approach — worth mentioning in an interview as the more mature alternative to clicking through the UI:

```bash
databricks jobs create --json '{
  "name": "retail_lakehouse_pipeline",
  "tasks": [
    {
      "task_key": "bronze",
      "notebook_task": {"notebook_path": "/Repos/<you>/enterprise-retail-lakehouse/notebooks/jobs/01_bronze"},
      "existing_cluster_id": "<your-cluster-id>"
    },
    {
      "task_key": "silver",
      "depends_on": [{"task_key": "bronze"}],
      "notebook_task": {"notebook_path": "/Repos/<you>/enterprise-retail-lakehouse/notebooks/jobs/02_silver"},
      "existing_cluster_id": "<your-cluster-id>"
    },
    {
      "task_key": "gold",
      "depends_on": [{"task_key": "silver"}],
      "notebook_task": {"notebook_path": "/Repos/<you>/enterprise-retail-lakehouse/notebooks/jobs/03_gold"},
      "existing_cluster_id": "<your-cluster-id>"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}'
```

This JSON definition could itself live in the repo (e.g. `dags/databricks_job_definition.json`) and be applied via CI on merge to `main` — the natural next step if you wanted true CI/CD for the orchestration layer itself, not just the pipeline code (see `docs/deployment_guide.md`'s "CI/CD Beyond This Project's Scope" section for the fuller picture).

## Comparing This to the Airflow DAG (`dags/retail_lakehouse_dag.py`)

| Aspect | Databricks Workflows | Airflow DAG (this repo) |
|---|---|---|
| Task dependency graph | Configured via UI or Jobs API JSON | Configured via `>>` operators in Python |
| Dynamic task generation from `table_config.yaml` | Not directly — Workflows tasks are notebooks, not per-table Python callables the way the DAG's loop generates them | Native — the DAG's `for table_name in list_configured_tables()` loop generates one Bronze+Silver task pair per config entry automatically |
| Retry/alerting | UI-configured per task | Code-configured via `DEFAULT_ARGS` |
| Best fit | Running natively inside Databricks, simplest operational path | Demonstrating orchestration-as-code skills, or environments spanning multiple compute platforms beyond just Databricks |

**Worth stating plainly if asked:** the Airflow DAG's dynamic per-table task generation (one Bronze/Silver task pair automatically created per `table_config.yaml` entry) is the more architecturally elegant demonstration of the config-driven design principle — the three-notebook Databricks Job above is coarser-grained (one task per *layer*, not per *table*), which is a reasonable, idiomatic trade-off for Databricks but doesn't showcase that specific pattern as directly. Both are legitimate; naming the difference explicitly is itself a good interview answer.
