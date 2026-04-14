# Databricks notebook source
# Instalação de dependências necessárias para arquivos Excel
%pip install openpyxl


# COMMAND ----------

# Reinicia o interpretador Python para carregar a biblioteca openpyxl instalada
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Context Configuration
catalog_name = "ems_case"
schema_raw = f"{catalog_name}.raw"
path_landing = f"/Volumes/{catalog_name}/raw/landing_zone/"

# COMMAND ----------

def add_metadata(df):
    """Adds auditing columns using Unity Catalog metadata."""
    return df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
             .withColumn("_source_file", F.col("_metadata.file_path"))

# COMMAND ----------

# -------------------------------------------------------------------------
# Ingestion: CSV Data with Automatic Delimiter Detection
# -------------------------------------------------------------------------

csv_files = ["erp_pedidos_cabecalho_2025", "erp_pedidos_itens_2025", "vendedores"]

def detect_csv_separator(file_path):
    """Peek at the first line to determine if the separator is ';' or ','."""
    try:
        # Lemos apenas a primeira linha via filesystem local (Volumes)
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            # Conta as ocorrências dos delimitadores
            if first_line.count(';') > first_line.count(','):
                return ';'
            else:
                return ','
    except Exception:
        return ',' # Fallback padrão

for file_name in csv_files:
    full_path = f"{path_landing}{file_name}.csv"
    
    # Detecção dinâmica do separador
    detected_sep = detect_csv_separator(full_path)
    
    # Leitura utilizando o separador identificado
    df_csv = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", detected_sep) \
        .load(full_path) \
        .select("*", "_metadata")
    
    # Aplicação de metadados e persistência na camada Raw
    df_raw = add_metadata(df_csv).drop("_metadata")
    df_raw.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{schema_raw}.{file_name}")

print(f"Ingestion of CSV files to {schema_raw} completed using dynamic separator detection.")

# COMMAND ----------

# CRM & Channels (Excel via Pandas Bridge)
excel_files = ["comercial_canais", "crm_clientes_export"]

for file in excel_files:
    local_path = f"{path_landing}{file}.xlsx"
    
    # O motor 'openpyxl' será chamado internamente pelo pandas
    pdf = pd.read_excel(local_path)
    
    # Tratamento de nulos e conversão para Spark
    df_spark = spark.createDataFrame(pdf.fillna("").astype(str))
    
    # Injeção de metadados manuais para fontes Pandas
    df_raw = df_spark.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                     .withColumn("_source_file", F.lit(local_path))
    
    df_raw.write.format("delta").mode("overwrite").saveAsTable(f"{schema_raw}.{file}")

print("Ingestão de arquivos Excel concluída com sucesso.")

# COMMAND ----------

# Legado Regiões (Pipe Separated)
df_regioes = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", "|") \
    .load(f"{path_landing}legado_regioes_pipe.txt") \
    .select("*", "_metadata")

# Chama a função definida anteriormente
add_metadata(df_regioes).drop("_metadata").write.format("delta") \
    .mode("overwrite").saveAsTable(f"{schema_raw}.legado_regioes")

# CRM & Channels (Excel via Pandas Bridge)
excel_files = ["comercial_canais", "crm_clientes_export"]

for file in excel_files:
    local_path = f"{path_landing}{file}.xlsx"
    pdf = pd.read_excel(local_path)
    df_spark = spark.createDataFrame(pdf.fillna("").astype(str))
    
    # Para Pandas, os metadados são inseridos manualmente
    df_raw = df_spark.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                     .withColumn("_source_file", F.lit(local_path))
    
    df_raw.write.format("delta").mode("overwrite").saveAsTable(f"{schema_raw}.{file}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {schema_raw}"))