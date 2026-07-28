# Troubleshooting Guide

## Environment Issues

| Symptom | Likely Cause | Fix |
|---|---|---|
| `JavaGatewayException` on `SparkSession` creation | Missing/wrong Java version | Verify `JAVA_HOME`; reinstall Java 17 |
| `ModuleNotFoundError: delta` | `delta-spark` not installed or venv not activated | `pip install delta-spark`; confirm `.venv` is active |
| Delta writes succeed but reads show old data | Spark caching stale metadata | `spark.catalog.clearCache()` or restart the session |
| Airflow DAG not appearing in UI | DAG file not in `$AIRFLOW_HOME/dags/`, or has a syntax error | `airflow dags list-import-errors` |
| Databricks CLI `403` errors | Expired/incorrect personal access token | Regenerate token; re-run `databricks configure --token` |
| `black`/`flake8` disagree on line length | Default flake8 max-line-length (79) vs. black's 88 | Set `max-line-length = 88` in `.flake8` / `pyproject.toml` |

## Pipeline Issues

| Symptom | Likely Cause | Fix |
|---|---|---|
| `SchemaValidationError` on Bronze ingestion | Source file structure doesn't match `table_config.yaml`'s declared schema | Compare file headers/types against config; this is a fatal error by design — not retried |
| `SourceFileNotFoundError` | Source path in config doesn't exist, or file hasn't landed yet | Verify `source_path` in `table_config.yaml`; confirm the upstream extract actually ran |
| `ConfigError: No configuration found for table 'X'` | Table not defined in `table_config.yaml`, or a typo in the table name argument | Check `table_config.yaml`'s `tables:` block for the exact key |
| Delta `MERGE INTO` throws "multiple source rows matched the same target row" | Incoming batch has duplicate business keys not yet deduplicated | Confirm the calling code applies `_deduplicate_within_batch` (dimensions) or relies on `_deduplicate_on_business_key` (facts) before merging |
| `scd2_merge` isn't creating new history for a column change | The changed column isn't listed in `tracked_columns` for that table in config | Add the column to `tracked_columns` if the change is meant to be historized |
| Point-in-time join in Gold returns unexpectedly `NULL` surrogate keys | Fact row's business key doesn't resolve to any dimension row (referential integrity gap — see `testing_strategy.md`) | Investigate whether this is a legitimate orphaned record upstream, or a join key mismatch |
| Currently-open dimension version never matches in the point-in-time join | `effective_end_date IS NULL` wasn't coalesced to a sentinel date before the range comparison | Confirm `_point_in_time_join` in `gold_builder.py` includes the `F.coalesce(...)` step |
| Retry loop exhausts on what looks like a fatal error | Exception type incorrectly classified as `TransientPipelineError` when it should be `FatalPipelineError` | Review the exception hierarchy in `exception_handler.py`; reclassify if needed |
| Silver processing shows `incremental_count: 0` unexpectedly | Watermark (Silver's max `_ingest_date`) is already ahead of available Bronze data | Confirm Bronze actually loaded new data for that date before assuming a bug |

## Data Quality Issues

| Symptom | Likely Cause | Fix |
|---|---|---|
| Pass rate unexpectedly low (`WARNING` logged) | Upstream source data genuinely degraded, or a DQ rule is miscalibrated | Inspect the quarantine table's captured violation reasons before assuming either cause |
| `no_dq_rules_configured` warning | `dq_rules` missing from a table's config entry | Add rules, or confirm intentionally omitted (rare — flag in code review) |
| Unknown rule type error | Typo in `type:` field in `dq_rules` config, or rule not yet implemented | Check `RULE_REGISTRY` in `data_quality.py` for available rule types |

## CI Issues

| Symptom | Likely Cause | Fix |
|---|---|---|
| CI fails on coverage threshold | New code added without corresponding tests | Add tests for the new logic; do not lower the threshold to pass |
| CI fails on `black --check` | Code not formatted before commit | Run `black src/ tests/` locally before pushing |
| CI passes locally but fails in GitHub Actions | Environment difference (Java version, missing constraint file for Airflow) | Compare `ci.yml`'s setup steps against local `developer_guide.md` setup exactly |

## When None of the Above Applies

1. Check the structured JSON log for the exact event name and any `error` field — every module logs enough context to start diagnosis without re-running
2. Reproduce against the small fixture data in `tests/fixtures/sample_data/` before debugging against full-scale data — isolates whether the issue is data-specific or logic-specific
3. If genuinely new, add a regression test reproducing it once resolved (see `testing_strategy.md` Section 5)
