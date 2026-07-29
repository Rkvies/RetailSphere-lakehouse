# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1 — Path setup
# MAGIC Run this first, in every new cluster session. Databricks Repos checks
# MAGIC your repo out under `/Workspace/Repos/<your-email>/<repo-name>/` (or
# MAGIC `/Repos/...` on older workspaces) — that path needs to be on `sys.path`
# MAGIC before `from src.common...` imports will resolve, since Databricks
# MAGIC doesn't auto-add your repo root the way a local `pip install -e .` would.

# COMMAND ----------

import sys
import os

# %pwd shows your current notebook location - if you open a notebook that
# lives INSIDE the repo (recommended), this already resolves correctly.
# If it doesn't print your repo path, replace the string below manually
# with the path shown in Repos > your repo > "Copy path".
repo_root = os.path.dirname(os.getcwd())
if repo_root not in sys.path:
    sys.path.append(repo_root)

print("Repo root added to sys.path:", repo_root)
print(sys.path[-1])
