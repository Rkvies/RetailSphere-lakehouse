# Developer Guide

## 1. Environment Setup

### Python & Virtual Environment
```bash
python3 --version   # 3.10 or 3.11
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
```

### Java (required by Spark)
Spark 3.5.x requires Java 8, 11, or 17.
```bash
java -version
# macOS: brew install openjdk@17
# Linux: sudo apt install openjdk-17-jdk
export JAVA_HOME=$(/usr/libexec/java_home -v17)   # macOS
```
**Common mistake:** installing Java 21 "because it's newer" — Spark 3.5.x does not officially support it across all configurations. Match Java version to your Spark/Databricks Runtime's documented support.

### Spark & Delta Lake (local dev)
```bash
pip install -r requirements.txt
python scripts/validate_env.py   # confirms Delta Lake writes/reads work locally
```

### Databricks Community Edition
1. Sign up at community.cloud.databricks.com (free, no card required)
2. Create a cluster on the latest LTS runtime (Delta pre-installed)
3. Optional: install the CLI for version-controllable workflows
```bash
pip install databricks-cli
databricks configure --token
```

### Git & GitHub
```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
git init && git branch -M main
```

### VS Code
Recommended extensions: Python, Pylance, Black Formatter, Flake8, YAML (Red Hat), GitLens, Databricks (official).
`.vscode/settings.json` is committed to the repo so formatting/linting behavior is consistent regardless of who's developing.

### Airflow (local, lightweight)
```bash
AIRFLOW_VERSION=2.9.1
PYTHON_VERSION="$(python --version | cut -d " " -f2 | cut -d "." -f1-2)"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
```
**Always use the official constraints file** — Airflow's dependency pinning is notoriously strict; skipping this is the most common source of install failures.

## 2. Coding Standards

- PEP-8, enforced via `black` (line length 88) and `flake8`
- Type hints on all function signatures
- Google-style docstrings on public functions/classes
- No bare `except:` — always catch specific exceptions, log before re-raising
- No magic strings/numbers — table names, thresholds, paths come from config
- Single-responsibility functions

## 3. Naming Standards

| Object | Convention | Example |
|---|---|---|
| Python modules | `snake_case` | `bronze_loader.py` |
| Classes | `PascalCase` | `DataQualityValidator` |
| Delta tables | `layer.entity_name` | `silver.dim_customer` |
| Fact tables | `fact_<subject>` | `fact_sales` |
| Dimension tables | `dim_<subject>` | `dim_product` |
| Surrogate keys | `<entity>_sk` | `customer_sk` |
| Metadata columns | `_<name>` prefix | `_ingest_ts`, `_source_file` |

## 4. Git & Commit Standards

Trunk-based-lite: feature branches off `main`, merged via PR (self-reviewed for a solo project).
```
feature/scd2-customer-dimension
fix/silver-dedup-null-handling
```

Conventional Commits:
```
feat: add SCD Type 2 handler for dim_customer
fix: correct null handling in deduplication logic
test: add unit tests for data quality validator
docs: add data dictionary for gold layer
```

## 5. Configuration Principles

No table name, path, business key, or partition column is ever hardcoded in a `.py` file. All table-specific behavior lives in `config/table_config.yaml`; environment-specific values (dev/prod) are layered via `config/base_config.yaml` + `config/<env>_config.yaml`, merged at runtime based on the `ENV` variable. See `src/common/config_loader.py` and its docstrings for the deep-merge mechanics.

## 6. Adding a New Data Domain (the practical test of the architecture)

1. Add an entry to `config/table_config.yaml`: `source_path`, `schema`, `business_key`, `scd_type` (or `null` for facts), `dq_rules`, and (if `scd_type: 2`) `tracked_columns` + `surrogate_key_col`
2. Drop a source file matching the declared schema into the configured `source_path`
3. Run `python -m src.pipelines.run_bronze_pipeline` — no code changes required
4. The Airflow DAG picks up the new table automatically on next parse (tasks are generated dynamically from config)

If step 3 requires touching any `.py` file, that's a regression in the config-driven design — worth filing an issue against.

## 7. Troubleshooting

See [`troubleshooting.md`](troubleshooting.md) for environment and pipeline-specific issues.
