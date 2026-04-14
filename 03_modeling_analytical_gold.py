# Databricks notebook source
catalog    = "ems_case" 
db_refined = f"{catalog}.refined"
db_gold    = f"{catalog}.gold"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ##dim_locations_gold
# MAGIC #####Unifica Vendedores, Canais e Regiões. Resolve a segmentação comercial.

# COMMAND ----------

df_dim_locations = spark.sql(f"""
    SELECT 
        s.seller_id,
        COALESCE(NULLIF(TRIM(s.seller_name), ''), 'NÃO INFORMADO') AS seller_name,
        COALESCE(NULLIF(TRIM(s.canal_id), ''), 'NÃO INFORMADO') AS canal_id,
        COALESCE(NULLIF(TRIM(r.regional_name), ''), 'NÃO INFORMADO') AS regional_name,
        COALESCE(NULLIF(TRIM(r.regional_manager), ''), 'NÃO INFORMADO') AS regional_manager,
        COALESCE(NULLIF(TRIM(r.state), ''), 'NÃO INFORMADO') AS uf,
        COALESCE(NULLIF(TRIM(s.regional_code), ''), 'NÃO INFORMADO') AS regional_code
    FROM {db_refined}.dim_sellers s
    LEFT JOIN {db_refined}.dim_regions r ON s.regional_code = r.regional_code
""")

df_dim_locations.write.format("delta").mode("overwrite").saveAsTable(f"{db_gold}.dim_locations_gold")

print("Tabela dim_locations_gold criada com tratamento de nulos!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##dim_products_gold
# MAGIC #####Traz a hierarquia de categorias para análise de sortimento.

# COMMAND ----------

df_dim_products = spark.sql(f"""
    SELECT 
        product_code AS product_id,
        COALESCE(NULLIF(TRIM(product_name), ''), 'NÃO INFORMADO') AS product_name,
        COALESCE(NULLIF(TRIM(category), ''), 'NÃO INFORMADO') AS category,
        COALESCE(NULLIF(TRIM(subcategory), ''), 'NÃO INFORMADO') AS subcategory,
        COALESCE(NULLIF(TRIM(product_status), ''), 'NÃO INFORMADO') AS product_status
    FROM {db_refined}.dim_products
""")

df_dim_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_gold}.dim_products_gold")

print("Tabela dim_products_gold atualizada com sucesso (Schema sobrescrito)!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##dim_calendar

# COMMAND ----------

df_calendar = spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) as date") \
    .select(
        F.col("date").alias("date_key"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter")
    )
df_calendar.write.format("delta").mode("overwrite").saveAsTable(f"{db_gold}.dim_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##fct_sales_master
# MAGIC #####A tabela principal do projeto. Une Vendas e Logística.
# MAGIC
# MAGIC - KPIs inclusos: Lead Time, Flag de Cancelamento, Valor Total. 
# MAGIC - Granularidade: Item de Pedido.

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Lendo as tabelas da camada REFINED
df_sales = spark.read.table(f"{db_refined}.fact_sales")

# Lemos a logística e removemos duplicatas de order_id para evitar o efeito "Fan-out" (duplicata de registros)
df_log = spark.read.table(f"{db_refined}.fact_logistics").dropDuplicates(["order_id"])
# ------------------------------

# Função para limpar strings e tratar campos vazios/nulos nas dimensões da fato
def clean_str(col_name):
    return F.coalesce(F.nullif(F.trim(F.col(col_name)), F.lit("")), F.lit("NÃO INFORMADO"))

# Unindo Vendas e Logística (Agora com a logística garantidamente única por pedido)
df_fct_sales = df_sales.join(df_log, "order_id", "left") \
    .select(
        # IDs e Datas originais da Refined
        df_sales["order_id"],
        df_sales["order_date"],
        df_sales["customer_id"],
        df_sales["seller_id"],
        df_sales["product_id"],
        
        # Campos de Texto Higienizados
        clean_str("status_item").alias("status_item"), 
        clean_str("shipping_mode").alias("shipping_mode"),
        clean_str("carrier_name").alias("carrier_name"),
        
        # Métricas Financeiras e Logísticas (Garantindo que frete nulo vire 0)
        F.coalesce(df_log["shipping_cost"], F.lit(0)).alias("shipping_cost"),
        df_sales["quantity"],
        df_sales["total_item_value"],
        
        # Datas de Logística
        df_log["dispatched_at"],
        df_log["delivered_at"],

        # --- KPIs DE NEGÓCIO (Calculados na Gold) ---
        
        # 1. Lead Time em dias
        F.datediff("delivered_at", "dispatched_at").alias("lead_time_days"),
        
        # 2. Flag de Cancelamento
        F.when(F.col("status_item") == "CANCELADO", 1)
         .otherwise(0).alias("is_cancelled"),
        
        # 3. Receita Líquida
        F.when(F.col("status_item") != "CANCELADO", F.col("total_item_value"))
         .otherwise(0).alias("net_revenue")
    )

# Escrita física na camada GOLD com overwriteSchema para limpar a "sujeira" anterior
df_fct_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_gold}.fct_sales_master")

print(f"Sucesso! Tabela fct_sales_master corrigida.")
print(f"Volume final na Gold: {df_fct_sales.count()} registros ")

# COMMAND ----------

# MAGIC %md
# MAGIC ##fct_customer_experience
# MAGIC #####Focada em Atendimento e Saúde do Cliente. Cruza Ocorrências com a Fato de Vendas para ver o impacto financeiro.

# COMMAND ----------

from pyspark.sql import functions as F

df_atend = spark.read.table(f"{db_refined}.fact_occurrences")

# lógica de limpeza para campos descritivos
def clean_str(col_name):
    return F.coalesce(F.nullif(F.trim(F.col(col_name)), F.lit("")), F.lit("NÃO INFORMADO"))

df_fct_exp = df_atend.select(
    F.col("ticket_id"),
    F.col("order_id"),
    F.col("customer_id"), # Original para auditoria de integridade
    F.col("created_at"),
    
    # Dimensão dentro da fato
    clean_str("occurrence_type").alias("occurrence_type"),
    clean_str("severity").alias("severity"),
    clean_str("ticket_status").alias("ticket_status"),
    
    # KPI de Severidade
    F.when(F.col("severity") == "HIGH", 1).otherwise(0).alias("is_critical")
)

df_fct_exp.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_gold}.fct_customer_experience")

print("Tabela fct_customer_experience criada com sucesso!")

# COMMAND ----------

from pyspark.sql import functions as F

# Lendo da Refined e salvando na Gold
df_sellers_refined = spark.table(f"{db_refined}.dim_sellers")

df_sellers_refined.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_gold}.dim_sellers")

print(f"Tabela {db_gold}.dim_sellers criada com sucesso!")