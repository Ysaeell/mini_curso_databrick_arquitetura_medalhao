# Databricks notebook source
# MAGIC %md
# MAGIC ###Criando Banco de dados (lhdw_vendas)

# COMMAND ----------

# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS lhdw_vendas")

# Usar o banco de dados
spark.sql("USE lhdw_vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando tabelas Delta

# COMMAND ----------

delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_produto/"

df = spark.read.format("delta").load(delta_table_path)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_produto")

# COMMAND ----------

# ── dim_categoria ─────────────────────────────────────────────
delta_table_path =  "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_categoria"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_categoria")

# COMMAND ----------

# ── dim_segmento ──────────────────────────────────────────────
delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_segmento"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_segmento")

# COMMAND ----------

# ── dim_fabricante ────────────────────────────────────────────
delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_fabricante"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_fabricante")

# COMMAND ----------

# ── dim_geografia ─────────────────────────────────────────────
delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_geografia"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_geografia")

# COMMAND ----------

# ── dim_cliente ───────────────────────────────────────────────
delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_cliente"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.dim_cliente")

# COMMAND ----------

# ── fato_vendas ───────────────────────────────────────────────
delta_table_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/fato_vendas"
df = spark.read.format("delta").load(delta_table_path)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("lhdw_vendas.fato_vendas")

# COMMAND ----------

# ── Verificação final ─────────────────────────────────────────
spark.sql("SHOW TABLES IN lhdw_vendas").show()

# COMMAND ----------

# ── Limpeza de memória ────────────────────────────────────────
import gc
spark.catalog.clearCache()
gc.collect()