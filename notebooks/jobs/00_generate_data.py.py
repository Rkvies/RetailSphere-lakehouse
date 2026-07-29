# Databricks notebook source
import sys, os
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))
if repo_root not in sys.path:
    sys.path.append(repo_root)
os.environ["ENV"] = "databricks"

from src.ingestion.data_generator import main as generate_data
generate_data()
