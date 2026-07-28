# Deployment Guide

This project runs entirely on free tooling for portfolio purposes. This guide documents how each component would map to a real production cloud deployment — a common and fair interview question ("this looks like a portfolio project, how would it actually run at scale?").

## Mapping Table

| This Project (Free Tier) | Production Equivalent | Why It Maps Cleanly |
|---|---|---|
| Local filesystem / `data/landing/` | S3 / ADLS Gen2 bucket with event notifications | Bronze ingestion already reads via a configured `source_path` — swapping to `s3a://` or `abfss://` paths requires only a config change |
| Databricks Community Edition (single node) | Databricks Job Clusters / EMR (autoscaling multi-node) | Same Spark/Delta APIs; `spark_session.py`'s shuffle-partition config is already environment-parameterized via `app_config` |
| Local Airflow (`airflow webserver` + `scheduler`) | Managed Airflow (MWAA) / Databricks Workflows | DAG structure is unchanged; only the execution environment differs |
| GitHub Actions (free tier) | Same GitHub Actions, or Jenkins/GitLab CI at enterprise scale | CI workflow (`ci.yml`) is portable as-is |
| `.env` / local environment variables | AWS Secrets Manager / Databricks secret scopes | Secrets were never committed to config — this is a drop-in replacement, not a redesign |
| Manual `pip install` dependency management | Docker image built from `docker/Dockerfile`, pushed to ECR/ACR | Dockerfile already exists for the data generator; the same pattern extends to pipeline containers |

## What Changes vs. What Doesn't

**Changes:**
- Storage paths (local → cloud object storage)
- Compute provisioning (manual local Spark → managed autoscaling clusters)
- Secrets management (`.env` → a managed secrets service)
- Orchestration execution environment (local Airflow → managed Airflow/Workflows)

**Does NOT change:**
- Any pipeline logic in `src/common/`, `src/ingestion/`, `src/transformation/`, `src/aggregation/`
- The DAG's dependency structure in `dags/retail_lakehouse_dag.py`
- `table_config.yaml`'s shape (only path values would change from local to cloud URIs)
- Data quality rules, SCD2 logic, or point-in-time join logic

This separation — business/pipeline logic untouched, only infrastructure configuration changing — is a direct result of the config-driven design established from Phase 5 onward, and is the strongest practical argument for that design choice.

## At Higher Volume: What Would Need Genuine Redesign (Not Just Reconfiguration)

Named honestly, not glossed over:
- `gold_builder.py`'s `mode("overwrite")` full rebuild would need to become incremental at true production fact-table volume
- Multiple `.count()` calls in `data_quality.py`'s `validate()` would need consolidation into fewer Spark actions
- Referential integrity between Gold facts and dimensions (currently unvalidated — see `testing_strategy.md`) would need an explicit monitoring/alerting layer before being trusted at scale
- Streaming ingestion, if adopted, requires the checkpoint-location and `readStream` changes described in `architecture.md`, plus a redesign of the Silver watermark mechanism (which currently assumes discrete daily batches)

## CI/CD Beyond This Project's Scope (Documented, Not Built)

A full production CI/CD pipeline would add, beyond this project's current `ci.yml`:
- Automated deployment to a staging Databricks workspace on merge to `main`
- Integration tests running against a real (non-Community-Edition) cluster before production promotion
- Automated DAG deployment sync (e.g., via the Databricks CLI or a dedicated Airflow DAG-sync pipeline)
- Cost monitoring/alerting on cluster spend
