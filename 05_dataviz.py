# Databricks notebook source
# Setup
catalog    = "ems_case" 
db_raw     = f"{catalog}.raw"
db_refined = f"{catalog}.refined"
db_gold    = f"{catalog}.gold"

from pyspark.sql import functions as F

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.vw_analytics_master AS
SELECT 
    -- Dimensão Tempo
    c.year,
    c.month_name,
    c.quarter,
    
    -- Dimensão Produto
    p.product_name,
    p.category,
    p.subcategory,
    
    -- Dimensão Comercial e Regional
    s.canal_name as channel_name,
    l.regional_name as region_name,
    l.uf as state,
    
    -- Fato Vendas e Métricas Base
    f.order_id,
    f.order_date,
    f.net_revenue,
    
    f.quantity,
    
    f.total_item_value,
    f.shipping_cost,
    
    -- KPIs Operacionais
    f.is_cancelled,
    f.lead_time_days,
    CASE WHEN f.lead_time_days > 3 THEN 1 ELSE 0 END as is_delayed,
    
    -- Cálculo de Ticket Médio: Protegido contra divisão por zero e formatado para evitar notação científica
    CAST(
        CASE 
            WHEN f.total_item_value <> 0 THEN f.total_item_value / f.quantity 
            ELSE 0 
        END 
    AS DECIMAL(18,2)
    CAST(
        CASE 
            WHEN f.quantity <> 0 THEN f.total_item_value / f.quantity 
            ELSE 0 
        END 
    AS DECIMAL(18,2)) as ticket_avg_item

FROM {db_gold}.fct_sales_master f
LEFT JOIN {db_gold}.dim_calendar c ON f.order_date = c.date_key
LEFT JOIN {db_gold}.dim_products_gold p ON f.product_id = p.product_id
LEFT JOIN {db_gold}.dim_sellers s ON f.seller_id = s.seller_id 
LEFT JOIN {db_gold}.dim_locations_gold l ON s.regional_code = l.regional_code
""")

print("View atualizada:")

# COMMAND ----------

display(spark.table(f"{db_gold}.vw_analytics_master"))