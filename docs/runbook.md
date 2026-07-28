# Runbook

Operational procedures for running, monitoring, and recovering the Enterprise Retail Lakehouse pipeline.

## 1. Running the Full Pipeline

**Manual (local development):**
```bash
python -m src.pipelines.run_bronze_pipeline
python -m src.pipelines.run_silver_pipeline
python -m src.pipelines.run_gold_pipeline
```

**Via Airflow (recommended — enforces correct dependency order):**
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow dags test retail_lakehouse_pipeline $(date +%F)
```

## 2. Running a Single Table

```python
from src.ingestion.bronze_loader import run_bronze_ingestion
from src.transformation.silver_processor import process_fact_table
from src.transformation.scd2_handler import process_dimension_table

run_bronze_ingestion("sales")
process_fact_table("sales")          # for fact tables
process_dimension_table("customer")  # for SCD2 dimension tables
```

## 3. Reprocessing a Specific Date (FR-08)

Bronze is partitioned by `_ingest_date`, making a single day's data isolable:
```python
# Delete only the affected partition, then re-run ingestion for that date's source file
spark.sql("DELETE FROM delta.`data/bronze/sales` WHERE _ingest_date = '2026-07-20'")
run_bronze_ingestion("sales")   # re-reads the source file for that date
```
Because `merge_upsert()` and `scd2_merge()` are idempotent, re-running Silver/Gold after this is always safe — no manual cleanup needed downstream of Bronze.

**Airflow backfill (for a range of dates):**
```bash
airflow dags backfill -s 2026-07-01 -e 2026-07-05 retail_lakehouse_pipeline
```

## 4. Monitoring

All pipeline stages emit structured JSON logs via `src/common/logger.py`. Key events to watch:

| Log Event | Level | Meaning |
|---|---|---|
| `bronze_ingestion_complete` | INFO | Includes `pass_rate` — watch for drops below 95% |
| `data_quality_validation_complete` | WARNING (if pass_rate < 95%) | Signals a data quality degradation worth investigating |
| `retry_attempt` | WARNING | A transient failure occurred and is being retried — frequent occurrences may indicate an upstream reliability issue |
| `retry_exhausted` | ERROR | A transient failure did not resolve after max retries — requires investigation |
| `no_dq_rules_configured` | WARNING | A table has no DQ rules defined — likely a config oversight |
| `silver_fact_processing_skipped` / `silver_dimension_processing_skipped` | INFO | No new Bronze data — normal on days with no upstream activity |

In production, these structured logs would feed CloudWatch/Datadog/ELK with alerts on `ERROR`-level events and on `pass_rate` dropping below threshold.

## 5. Recovery Procedures

### A pipeline task failed
1. Check the structured log for the specific exception type (fatal vs. transient — see `troubleshooting.md`)
2. Fatal errors (`SchemaValidationError`, `ConfigurationError`) require a code/config/data fix before retry will help
3. Transient errors typically self-resolve via the built-in `@retry` decorator; if `retry_exhausted` appears, investigate the underlying system (source file availability, cluster health) before manually retrying

### Data quality pass rate dropped significantly
1. Query the quarantine table for the affected domain: `spark.read.format("delta").load("data/quarantine/<table>")`
2. Review violation reasons (captured per-row in the DQ engine's `RuleViolation` output)
3. Determine if this is an upstream data issue (contact source system owner) or a DQ rule that's too strict (review `table_config.yaml`)

### Suspected bad Gold-layer data from a dimension change
1. Query `dim_customer`/`dim_product` directly, filtering on `is_current = false`, to inspect historical versions
2. Verify the point-in-time join is resolving correctly for a specific known transaction (see `gold_builder.py`'s manual verification query in its module documentation)

## 6. Performance Benchmarking

Run before major changes to Gold-layer query patterns:
```bash
python scripts/benchmark_gold_queries.py
```
Compare output against the last documented baseline. See `testing_strategy.md` Section 6 for the benchmarking policy.

## 7. Adding a New Data Domain

See `developer_guide.md` Section 6 — config-only change, no pipeline code modification required.

## 8. Known Operational Gaps

- Referential integrity between Gold facts and dimensions is not actively monitored — unmatched joins currently silently resolve to `NULL`. Recommended manual check periodically: count Gold rows with `NULL` surrogate keys.
- No automated alerting is wired up in this portfolio deployment; log-based monitoring is manual. Production deployment would wire structured logs into an alerting pipeline (see `deployment_guide.md`).
