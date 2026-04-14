# Databricks notebook source
catalog    = "ems_case" 
db_refined = f"{catalog}.refined"
db_gold    = f"{catalog}.gold"
db_raw = f"{catalog}.raw"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_refined}")

from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql import functions as F

# Analisando gaps na dimensão de produtos
df_products_check = spark.table(f"{db_refined}.dim_products").select(
    F.lit("DIM_PRODUCTS").alias("table_name"),
    F.col("product_code").alias("record_id"),
    F.when(F.col("subcategory").isNull(), "SUB CATEGORIA AUSENTE")
     .when(F.col("product_status").isNull(), "STATUS PRODUTO AUSENTE")
     .otherwise(None).alias("issue_description")
).filter("issue_description IS NOT NULL")

# Analisando gaps na fato de ocorrências (customer_id null)
df_occur_check = spark.table(f"{db_refined}.fact_occurrences").select(
    F.lit("FACT_OCCURRENCES").alias("table_name"),
    F.col("ticket_id").alias("record_id"),
    F.when(F.col("customer_id").isNull(), "CUSTOMER_ID ÓRFÃO (SEM VÍNCULO)")
     .otherwise(None).alias("issue_description")
).filter("issue_description IS NOT NULL")

# Unindo os gaps em uma tabela detalhada para o time de cadastro
df_detailed_gaps = df_products_check.union(df_occur_check)
df_detailed_gaps.write.format("delta").mode("overwrite").saveAsTable(f"{db_gold}.dq_detailed_gaps")

# COMMAND ----------

# Vendas que não possuem Seller correspondente na Dimensão
df_sales = spark.table(f"{db_refined}.fact_sales")
df_sellers = spark.table(f"{db_refined}.dim_sellers")

df_orphan_sellers = df_sales.join(df_sellers, "seller_id", "left_anti") \
    .select(
        F.lit("FACT_SALES").alias("table_name"),
        F.col("order_id").alias("record_id"),
        F.lit("VENDEDOR NÃO ENCONTRADO NA DIMENSÃO").alias("issue_description")
    )

# Adicionando à tabela de gaps detalhados
df_orphan_sellers.write.format("delta").mode("append").saveAsTable(f"{db_gold}.dq_detailed_gaps")

# COMMAND ----------

# 1. Calculo dos valores 
raw_items_count = spark.table(f"{db_raw}.erp_pedidos_itens_2025").count()
gold_sales_count = spark.table(f"{db_gold}.fct_sales_master").count()

# 2. Status
status_vendas = "ACEITÁVEL (DEDUPLICAÇÃO/ÓRFÃOS)" if 900 < gold_sales_count <= raw_items_count else "ERRO CRÍTICO"

# 3. DataFrame de conciliação
df_volumetria = spark.createDataFrame([
    ("VENDAS_ITENS", raw_items_count, gold_sales_count, raw_items_count - gold_sales_count, status_vendas)
], ["processo", "qtd_raw", "qtd_gold", "diferenca", "parecer_tecnico"])

# 4. Tabela de qualidade
df_volumetria.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_gold}.dq_conciliacao_vendas")

print(f"Check concluído!")
print(f"Raw: {raw_items_count} | Gold: {gold_sales_count} | Status: {status_vendas}")

# COMMAND ----------

display(df_volumetria)