# Databricks notebook source
# -------------------------------------------------------------------------
# Setup - Infra
# -------------------------------------------------------------------------

# Configuration
catalog_name = "ems_case"
layers = ["raw", "refined"]
volume_name = "landing_zone"

# 1. Catalog Initialization
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
except Exception:
    # Fallback to hive_metastore if custom catalog creation is restricted
    catalog_name = "hive_metastore"
    spark.sql(f"USE CATALOG {catalog_name}")

# 2. Schema Creation
for layer in layers:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer}")

# 3. Volume Creation (Landing Zone)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.raw.{volume_name}")

# 4. Path Mapping for Ingestion Pipelines
path_landing = f"/Volumes/{catalog_name}/raw/{volume_name}/"

print(f"Infrastructure initialized at catalog: {catalog_name}")
print(f"Landing path: {path_landing}")