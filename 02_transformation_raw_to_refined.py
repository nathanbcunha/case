# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup e Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog = "ems_case"
db_raw = f"{catalog}.raw"
db_refined = f"{catalog}.refined"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_refined}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimensão Produtos (Tratando JSON da API)
# MAGIC #####Os campos pricing e attributes precisam ser lidos como JSON.

# COMMAND ----------

df_prod_raw = spark.read.table(f"{db_raw}.cadastro_produtos_api_dump")

# Aplicando UPPER e TRIM nas colunas de dimensão (texto)
df_dim_products = df_prod_raw.select(
    # IDs geralmente mantemos o padrão original, mas limpamos espaços
    F.trim(F.col("product.product_id")).alias("product_code"),
    
    # Dimensões de texto com UPPER e TRIM para consistência no BI
    F.upper(F.trim(F.col("product.name"))).alias("product_name"),
    F.upper(F.trim(F.col("product.category"))).alias("category"),
    F.upper(F.trim(F.col("product.subcategory"))).alias("subcategory"),
    F.upper(F.trim(F.col("product.status"))).alias("product_status"),
    
    # Campos numéricos e monetários
    F.expr("try_cast(pricing.list_price as decimal(18,2))").alias("price_reference"),
    F.upper(F.trim(F.col("pricing.currency"))).alias("currency"),
    
    # Timestamp (ISO)
    F.to_timestamp(F.col("updated_at")).alias("last_api_update")
)

# Salvando a tabela com a nova estrutura padronizada
df_dim_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.dim_products")

print("Sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimensão Clientes

# COMMAND ----------

df_clients_raw = spark.read.table(f"{db_raw}.crm_clientes_export")

df_dim_clients = df_clients_raw.select(
    
    F.trim(F.upper(F.col("customer_id"))).alias("customer_id"),
    F.upper(F.trim(F.col("nome_cliente"))).alias("customer_name"),
    F.upper(F.trim(F.col("segmento"))).alias("segment"),
    F.upper(F.trim(F.col("porte"))).alias("company_size"),
    F.upper(F.trim(F.col("cidade"))).alias("city"),
    
    # Normalização do Estado (Estado -> State)
    F.upper(F.trim(
        F.when(F.upper(F.col("estado")).isin("SC", "S. CATARINA", "STA CATARINA", "SANTA CATARINA"), "SANTA CATARINA")
         .when(F.upper(F.col("estado")).isin("PR", "PARANÁ", "PARANA"), "PARANA")
         .when(F.upper(F.col("estado")).isin("RJ", "RIO DE JANEIRO"), "RIO DE JANEIRO")
         .when(F.upper(F.col("estado")).isin("MG", "MINAS GERAIS"), "MINAS GERAIS")
         .when(F.upper(F.col("estado")).isin("SP", "SÃO PAULO", "SAO PAULO"), "SAO PAULO")
         .otherwise(F.col("estado"))
    )).alias("state")
).dropDuplicates(["customer_id"]) 

# Salvando
df_dim_clients.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.dim_clients")

print("Normalização Concluída! Dimensão Clientes está limpa e sem duplicatas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimensões Regiões

# COMMAND ----------

df_regioes = spark.read.table(f"{db_raw}.legado_regioes")

df_dim_regions = df_regioes.select(
    # 1. Chave de Ligação
    F.upper(F.trim(
        F.when(F.upper(F.col("regional_code")).isin("SUL", "S"), "S")
         .when(F.upper(F.col("regional_code")).isin("NORTE", "N"), "N")
         .when(F.upper(F.col("regional_code")).isin("NORDESTE", "NE"), "NE")
         .when(F.upper(F.col("regional_code")).isin("SUDESTE", "SE"), "SE")
         .when(F.upper(F.col("regional_code")).isin("CENTRO-OESTE", "CO"), "CO")
         .otherwise(F.col("regional_code"))
    )).alias("regional_code"),
    
    # 2. Flag de Status (Ajustado: agora usando o nome que o Spark sugeriu)
    F.upper(F.trim(F.col("active_flag"))).alias("active_flag"),
    
    # 3. Nome da Região e Gerente
    F.upper(F.trim(
        F.when(F.upper(F.col("regional_name")).contains("SUL"), "SUL")
         .otherwise(F.col("regional_name"))
    )).alias("regional_name"),
    
    F.upper(F.trim(F.col("manager_name"))).alias("regional_manager"),
    
    # 4. Estado (Normalização)
    F.upper(F.trim(
        F.when(F.upper(F.col("state")).isin("SANTA CATARINA", "STA CATARINA", "S. CATARINA", "SC"), "SC")
         .when(F.upper(F.col("state")).isin("SAO PAULO", "SÃO PAULO", "SP"), "SP")
         .when(F.upper(F.col("state")).isin("RIO DE JANEIRO", "RJ"), "RJ")
         .when(F.upper(F.col("state")).isin("MINAS GERAIS", "MG"), "MG")
         .when(F.upper(F.col("state")).isin("PARANÁ", "PARANA", "PR"), "PR")
         .otherwise(F.col("state"))
    )).alias("state")
    
).filter("regional_code != 'XX'") \
 .dropDuplicates(["regional_code", "state"])

# Salvando
df_dim_regions.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.dim_regions")

print("Agora sim! Dimensão Regiões sincronizada com os nomes da Raw.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fato Vendas (Capa + Itens)

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Lendo da RAW
df_header_raw = spark.read.table(f"{db_raw}.erp_pedidos_cabecalho_2025")
df_items = spark.read.table(f"{db_raw}.erp_pedidos_itens_2025")

# Garantimos que cada order_id apareça apenas UMA vez no cabeçalho
df_header = df_header_raw.dropDuplicates(["order_id"])
# ---------------------------

# 2. Fazendo o Join
df_fact_sales = df_items.join(df_header, "order_id", "left") \
    .select(
        F.upper(F.trim(F.col("order_id"))).alias("order_id"),
        
        # Tratamento de datas
        F.coalesce(
            F.expr("try_to_date(order_date, 'yyyy/MM/dd')"),
            F.expr("try_to_date(order_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(order_date, 'dd/MM/yyyy')")
        ).alias("order_date"),
        
        # IDs e Métricas
        F.upper(F.trim(F.col("customer_code"))).alias("customer_id"),
        F.upper(F.trim(F.col("seller_id"))).alias("seller_id"),
        F.upper(F.trim(F.col("product_code"))).alias("product_id"),
        F.expr("try_cast(quantity as int)").alias("quantity"),
        F.regexp_replace(F.col("unit_price"), ",", ".").cast("decimal(18,2)").alias("unit_price"),
        F.expr("try_cast(total_item as decimal(18,2))").alias("total_item_value"),
        
        # Status do Item (Nome atualizado para bater com a Gold)
        F.coalesce(F.upper(F.trim(F.col("item_status"))), F.lit("NÃO INFORMADO")).alias("status_item")
    )

# 3. Salvando na Refined
df_fact_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.fact_sales")

print(f"Refined corrigida! Volume: {df_fact_sales.count()} (Deve ser 995)")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fato Logística (Extraindo Timestamps)
# MAGIC #####O campo timestamps na logística é crucial para calcular atrasos.

# COMMAND ----------

df_log_raw = spark.read.table(f"{db_raw}.logistica_entregas")

df_fact_logistics = df_log_raw.select(
    F.upper(F.trim(F.col("delivery_id"))).alias("delivery_id"),
    F.upper(F.trim(F.col("order_ref"))).alias("order_id"),
    
    # Abrindo e Normalizando o Struct 'carrier'
    F.upper(F.trim(F.col("carrier.mode"))).alias("shipping_mode"),
    F.upper(F.trim(
        F.coalesce(F.col("carrier.name"), F.lit("NAO INFORMADO"))
    )).alias("carrier_name"),
    
    F.expr("try_cast(cost as decimal(18,2))").alias("shipping_cost"),
    
    # NORMALIZAÇÃO DO STATUS (UPPER + TRIM)
    F.upper(F.trim(F.col("delivery_status"))).alias("delivery_status"),
    
    # Tratamento de datas resiliente
    F.coalesce(
        F.expr("try_to_timestamp(timestamps.shipped_at, 'yyyy-MM-dd\\'T\\'HH:mm:ss')"),
        F.expr("try_to_timestamp(timestamps.shipped_at, 'dd/MM/yyyy HH:mm')"),
        F.expr("try_to_date(timestamps.shipped_at, 'yyyy-MM-dd')")
    ).alias("dispatched_at"),
    
    F.coalesce(
        F.expr("try_to_timestamp(timestamps.delivered_at, 'yyyy-MM-dd\\'T\\'HH:mm:ss')"),
        F.expr("try_to_timestamp(timestamps.delivered_at, 'dd/MM/yyyy HH:mm')"),
        F.expr("try_to_date(timestamps.delivered_at, 'yyyy-MM-dd')")
    ).alias("delivered_at")
)

# Salvando com overwriteSchema
df_fact_logistics.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.fact_logistics")

print("Tabela fact_logistics 100% padronizada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fato Ocorrências

# COMMAND ----------

df_atend_raw = spark.read.table(f"{db_raw}.atendimento_ocorrencias")

df_fact_occurrences = df_atend_raw.select(
    # IDs padronizados para garantir o JOIN com Vendas e Clientes
    F.upper(F.trim(F.col("ticket_id"))).alias("ticket_id"),
    F.upper(F.trim(F.col("order_id"))).alias("order_id"),
    F.upper(F.trim(F.col("customer_code"))).alias("customer_id"),
    
    # NORMALIZAÇÃO DOS CAMPOS DE TEXTO
    F.upper(F.trim(F.col("event_type"))).alias("occurrence_type"),
    F.upper(F.trim(F.col("severity"))).alias("severity"),
    F.upper(F.trim(F.col("status"))).alias("ticket_status"),
    
    # Mantendo o tratamento de data resiliente (ISO, BR e Date)
    F.coalesce(
        F.expr("try_to_timestamp(created_at, 'yyyy-MM-dd\\'T\\'HH:mm:ss')"),
        F.expr("try_to_timestamp(created_at, 'dd/MM/yyyy HH:mm')"),
        F.expr("try_to_date(created_at, 'yyyy-MM-dd')")
    ).alias("created_at")
)

# Salvando com overwrite Schema para garantir a nova estrutura
df_fact_occurrences.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.fact_occurrences")

print("Tabela fact_occurrences normalizada e salva com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimensão Vendedores e Canais
# MAGIC #####Unificando o cadastro de vendedores com os nomes dos canais de venda.

# COMMAND ----------

df_sellers = spark.read.table(f"{db_raw}.vendedores")
df_channels = spark.read.table(f"{db_raw}.comercial_canais")

# Join e tratamento robusto de datas
df_dim_sellers = df_sellers.join(df_channels, df_sellers["canal_id"] == df_channels["id_canal"], "left") \
    .select(
        F.upper(F.trim(df_sellers["seller_id"])).alias("seller_id"),
        F.upper(F.trim(F.col("seller_name"))).alias("seller_name"),
        F.upper(F.trim(F.col("id_canal"))).alias("canal_id"), 
        F.upper(F.trim(F.col("nome_canal"))).alias("canal_name"),
        F.upper(F.trim(F.col("status"))).alias("seller_status"),
        
        # Normalização do Regional Code
        F.upper(F.trim(
            F.when(F.upper(F.col("regional_code")).isin("SUL", "S"), "S")
             .when(F.upper(F.col("regional_code")).isin("NORTE", "N"), "N")
             .when(F.upper(F.col("regional_code")).isin("NORDESTE", "NE"), "NE")
             .when(F.upper(F.col("regional_code")).isin("SUDESTE", "SE"), "SE")
             .when(F.upper(F.col("regional_code")).isin("CENTRO-OESTE", "CO"), "CO")
             .otherwise(F.col("regional_code"))
        )).alias("regional_code"),
        
        
        F.coalesce(
            F.expr("try_to_date(hire_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(hire_date, 'dd/MM/yyyy')"),
            F.expr("try_to_date(hire_date, 'yyyy/MM/dd')")
        ).alias("hire_date")
    )

# Salvando com overwriteSchema
df_dim_sellers.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{db_refined}.dim_sellers")

print("Sucesso! Dimensão Vendedores salva com datas corrigidas.")