# How to Run This Project — Step by Step

This fills a gap in the phased build: a single, linear "clone to results" path.

## Step 0 — Prerequisites

- Java 17 installed (`java -version`) — **Java 21 will likely fail** to resolve the Delta JAR cleanly with delta-spark 3.1.0/Spark 3.5.1; use Java 17 specifically
- Python 3.10 or 3.11
- Normal internet access (Spark needs to download the Delta Lake JAR from Maven Central on first run — this only happens once, then it's cached locally)

## Step 1 — Environment Setup

```bash
git clone <your-repo-url> && cd enterprise-retail-lakehouse
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1

pip install pyspark==3.5.1 delta-spark==3.1.0 pyyaml python-dotenv faker pytest pytest-cov
```

## Step 2 — Validate Spark + Delta Actually Work

Create `scripts/validate_env.py` (from Phase 6) and run it:
```bash
python scripts/validate_env.py
```
Expect: a small table prints, followed by `Delta Lake local validation: SUCCESS`. **Don't proceed until this works** — every later step depends on it.

## Step 3 — Generate the Landing Zone Data

```bash
mkdir -p data/landing
python -m src.ingestion.data_generator
```
Expect: console output listing 10 files written under `data/landing/<domain>/`, ending in a confirmation that all foreign keys resolve. **This step must run before Step 4** — Bronze has nothing to read otherwise.

## Step 4 — Confirm Configuration Is Complete

```bash
python -c "
from src.common.config_loader import list_configured_tables
print(list_configured_tables())
"
```
Expect: all 10 table names printed. If this errors, check `config/table_config.yaml` exists and matches the complete version (not a partial excerpt).

## Step 5 — Run Bronze Ingestion

```bash
python -m src.pipelines.run_bronze_pipeline
```
Expect: one `bronze_ingestion_complete` JSON log line per table. Verify output:
```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format('delta').load('data/bronze/sales').show(5)
"
```

## Step 6 — Run Silver Processing

```bash
python -m src.pipelines.run_silver_pipeline
```
This routes each table to `process_fact_table()` or `process_dimension_table()` based on `scd_type` in config. Verify:
```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format('delta').load('data/silver/customer').show(5)
"
```
You should see `customer_sk`, `is_current`, `effective_start_date` columns populated.

## Step 7 — Run Gold Aggregation

```bash
python -m src.pipelines.run_gold_pipeline
```
Verify:
```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format('delta').load('data/gold/fact_sales').show(5)
"
```
You should see `customer_sk`, `product_sk`, `store_sk`, `line_total` — the fully joined star schema.

## Step 8 — Run the Test Suite

```bash
pytest tests/unit/ -v --cov=src --cov-report=term-missing
pytest tests/integration/ -v
```

## Step 9 (Optional) — Run via Airflow Instead of Manual Scripts

```bash
pip install "apache-airflow==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
mkdir -p $AIRFLOW_HOME/dags
cp dags/retail_lakehouse_dag.py $AIRFLOW_HOME/dags/
airflow dags test retail_lakehouse_pipeline $(date +%F)
```

## Step 10 — See a Real Result

The most convincing thing to actually look at:
```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format('delta').load('data/gold/fact_sales') \
    .groupBy('customer_sk') \
    .sum('line_total') \
    .orderBy('sum(line_total)', ascending=False) \
    .show(10)
"
```
This is a real query against a real, fully-built Gold table — top 10 customers by total spend, joined correctly across Bronze → Silver (with SCD2) → Gold.

## Troubleshooting This Specific Sequence

| If Step... | Fails with... | Because... |
|---|---|---|
| 2 | `JAVA_GATEWAY_EXITED` | Wrong Java version, or no internet access to resolve the Delta JAR on first run |
| 5 | `SourceFileNotFoundError` | Step 3 wasn't run, or `table_config.yaml`'s `source_path` doesn't match where the generator actually wrote files |
| 6 | `ConfigError: No configuration found` | `table_config.yaml` is missing an entry, or you're using a partial excerpt instead of the complete file |
| 7 | Gold table has all-`NULL` surrogate keys | Silver Step 6 didn't actually populate `dim_customer`/`dim_product`/`dim_store` — check Step 6's output first |

See `docs/troubleshooting.md` for the fuller reference.
