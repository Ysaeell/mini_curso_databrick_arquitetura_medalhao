# Databricks notebook source
# MAGIC %md
# MAGIC ### Camada Gold (Delta): Criação de Fatos e Dimensões

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession with the required configurations for Delta Lake
spark = SparkSession.builder \
    .appName("Carga Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Define os caminhos de armazenamento no Data Lake
silver_path = "/Volumes/workspace/lhdw/vendas_volume/silver/vendas"
gold_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ler dados Camada Silver

# COMMAND ----------

df_silver = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Produto

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
#Nome tabela destino

tb_destino = "dim_produto"

# Extrair produtos únicos para a dimensão Produto
dim_produto_df = df_silver.select(
    "IDProduto", "Produto", "Categoria").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_produto_df = dim_produto_df.withColumn("sk_produto", monotonically_increasing_id()+1)

# Escrever DimProduto no formato Delta
dim_produto_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

#display(dim_produto_df)
#dim_produto_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Categoria

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
#Nome tabela destino

tb_destino = "dim_categoria"

# Extrair Categorias únicas para a dimensão Categoria
dim_categoria_df = df_silver.select(
    "Categoria").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_categoria_df = dim_categoria_df.withColumn("sk_categoria", monotonically_increasing_id()+1)

# Escrever DimCatgoria no formato Parquet, particionando por Categoria
dim_categoria_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

dim_categoria_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Segmento

# COMMAND ----------

#Nome tabela destino

tb_destino = "dim_segmento"

# Extrair Segmentos únicos para a dimensão Segmentos
dim_segmento_df = df_silver.select(
   "Segmento").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_segmento_df = dim_segmento_df.withColumn("sk_segmento", monotonically_increasing_id()+1)

# Escrever DimSegmento no formato Parquet
dim_segmento_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

dim_segmento_df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Fabricante

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_fabricante"

# Extrair produtos únicos para a dimensão Fabricante    
dim_fabricante_df = df_silver.select(
    "IDFabricante", "Fabricante").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_fabricante_df = dim_fabricante_df.withColumn("sk_fabricante", monotonically_increasing_id()+1)

# Escrever DimFabricante no formato Delta
dim_fabricante_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

dim_fabricante_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Geografia

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_geografia"

# Extrair Geografia  únicos para a dimensão Geografia
dim_geografia_df = df_silver.select(
     "Cidade", "Estado", "Regiao", "Distrito", "Pais", "CodigoPostal"
).dropDuplicates()

# Adicionar chave substituta
dim_geografia_df = dim_geografia_df.withColumn("sk_geografia", monotonically_increasing_id()+1)

# Escrever DimGeografia no formato Parquet
dim_geografia_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

dim_geografia_df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Cliente

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_cliente"

from pyspark.sql.functions import col, monotonically_increasing_id
# Passo 1 - Extrair clientes únicos para a dimensão Cliente
dim_cliente_df = df_silver.select(
    "IDCliente", "Nome", "Email", "Cidade", "Estado", "Regiao", "Distrito", "Pais", "CodigoPostal"
).dropDuplicates()

# Passo 2 - Realizar o join para obter a SK_Geografia
dim_cliente_com_sk_df = dim_cliente_df.alias("cliente") \
    .join(dim_geografia_df.alias("geografia"), 
          (col("cliente.Cidade") == col("geografia.Cidade")) &
          (col("cliente.Estado") == col("geografia.Estado")) &
          (col("cliente.Regiao") == col("geografia.Regiao")) &
          (col("cliente.Distrito") == col("geografia.Distrito")) &
          (col("cliente.Pais") == col("geografia.Pais")) &
          (col("cliente.CodigoPostal") == col("geografia.CodigoPostal")), 
          "left") \
    .select("cliente.IDCliente", "cliente.Nome", "cliente.Email", "geografia.sk_geografia")

# Passo 3 - Adicionar chave substituta
dim_cliente_com_sk_df = dim_cliente_com_sk_df.withColumn("sk_cliente", monotonically_increasing_id()+1)

# Passo 4 - Selecionar colunas específicas
dim_cliente_com_sk_df = dim_cliente_com_sk_df.select("IDCliente", "Nome","Email", "sk_geografia", "sk_cliente")

# Passo 5 - Escrever DimCliente no formato Delta
dim_cliente_com_sk_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

#display(dim_cliente_com_sk_df)
#dim_cliente_com_sk_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação de Tabela Fato

# COMMAND ----------

#Nome tabela destino
tb_destino = "fato_vendas"

from pyspark.sql.functions import broadcast,year, month
# Juntar dados da Silver com tabelas de dimensões para obter as chaves substitutas
fato_vendas_df = df_silver.alias("s") \
    .join(broadcast(dim_produto_df.select("IDProduto", "sk_produto").alias("dprod")), "IDProduto") \
    .join(broadcast(dim_categoria_df.select("Categoria", "sk_categoria").alias("dcat")), "Categoria") \
    .join(broadcast(dim_segmento_df.select("Segmento", "sk_segmento").alias("dseg")), "Segmento") \
    .join(broadcast(dim_fabricante_df.select("Fabricante", "sk_fabricante").alias("dfab")), "Fabricante") \
    .join(broadcast(dim_cliente_com_sk_df.select("IDCliente", "sk_cliente").alias("dcli")), "IDCliente") \
    .select(
        col("s.Data").alias("DataVenda"),
        "sk_produto",
        "sk_categoria",
        "sk_segmento",
        "sk_fabricante",
        "sk_cliente",
        "Unidades",
        col("s.PrecoUnitario"),
        col("s.CustoUnitario"),
        col("s.TotalVendas")
    )

# Escrever tabela Fato no formato Delta, particionando por DataVenda (ano e mês)
fato_vendas_df.withColumn("Ano", year("DataVenda")) \
             .withColumn("Mes", month("DataVenda")) \
             .write.format("delta") \
             .mode("overwrite")\
             .option("MaxRecordsPerFile", 1000000)\
             .partitionBy("Ano", "Mes")\
             .save(f"{gold_path}/{tb_destino}")

fato_vendas_df.count()             

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza de Memória

# COMMAND ----------

import gc

# Coletar lixo após operações pesadas para liberar memória
gc.collect()

# Limpar todos os dados em cache
#spark.catalog.clearCache() #NÃO SE APLICA A SERVERLESS

# COMMAND ----------

# MAGIC %md
# MAGIC ###Evidências de Carga na Camada Gold (Delta)

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/dim_categoria/

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/fato_vendas/Ano=2012/