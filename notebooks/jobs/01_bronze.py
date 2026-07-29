# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.pipelines.run_bronze_pipeline import main as run_bronze
result = run_bronze()
print(result)
