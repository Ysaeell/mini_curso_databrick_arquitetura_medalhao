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
gold_fato_path = "/Volumes/workspace/lhdw/vendas_volume/gold/vendas_delta/fato_vendas"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ler dados Camada Silver
# MAGIC Filtrado pela maior data na tabela fato

# COMMAND ----------

from pyspark.sql.functions import date_sub, lit

# Ler a maior data de venda da tabela fato_vendas
max_data_venda = spark.read.format("delta").load(gold_fato_path) \
                          .selectExpr("max(DataVenda) as MaxDataVenda") \
                          .collect()[0]["MaxDataVenda"]

display(max_data_venda)

# Carregar dados da Silver filtrando pela DataVenda maior que a obtida acima
df_silver = spark.read.format("parquet").load(silver_path) \
                          .filter(f"Data > '{max_data_venda}'")

df_silver.count()                          

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD da Dimensão Produto

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_produto"
path_tabela = f"{gold_path}/{tb_destino}"

# Extrair produtos únicos
dim_produto_df = df_silver.select(
    "IDProduto",
    "Produto",
    "Categoria"
).dropDuplicates()

# Adicionar data de atualização
dim_produto_df = dim_produto_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_produto_df.alias("source"),
            "target.IDProduto = source.IDProduto"
        )
        .whenMatchedUpdate(
            set={
                "Produto": "source.Produto",
                "Categoria": "source.Categoria",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "IDProduto": "source.IDProduto",
                "Produto": "source.Produto",
                "Categoria": "source.Categoria",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_produto_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

#display(spark.read.format("delta").load(path_tabela))


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD da Dimensão Categoria

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_categoria"
path_tabela = f"{gold_path}/{tb_destino}"

# Extrair categorias únicas
dim_categoria_df = df_silver.select(
    "Categoria"
).dropDuplicates()

# Adicionar data de atualização
dim_categoria_df = dim_categoria_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_categoria_df.alias("source"),
            "target.Categoria = source.Categoria"
        )
        .whenMatchedUpdate(
            set={
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "Categoria": "source.Categoria",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_categoria_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

#display(spark.read.format("delta").load(path_tabela))


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD da Dimensão Segmento

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_segmento"
path_tabela = f"{gold_path}/{tb_destino}"

# Extrair segmentos únicos
dim_segmento_df = df_silver.select(
    "Segmento"
).dropDuplicates()

# Adicionar data de atualização
dim_segmento_df = dim_segmento_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_segmento_df.alias("source"),
            "target.Segmento = source.Segmento"
        )
        .whenMatchedUpdate(
            set={
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "Segmento": "source.Segmento",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_segmento_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

#display(spark.read.format("delta").load(path_tabela))


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD da Dimensão Fabricante

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_fabricante"
path_tabela = f"{gold_path}/{tb_destino}"

# Extrair fabricantes únicos
dim_fabricante_df = df_silver.select(
    "IDFabricante",
    "Fabricante"
).dropDuplicates()

# Adicionar data de atualização
dim_fabricante_df = dim_fabricante_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_fabricante_df.alias("source"),
            "target.IDFabricante = source.IDFabricante"
        )
        .whenMatchedUpdate(
            set={
                "Fabricante": "source.Fabricante",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "IDFabricante": "source.IDFabricante",
                "Fabricante": "source.Fabricante",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_fabricante_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

#display(spark.read.format("delta").load(path_tabela))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD da Dimensão Geografia

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_geografia"
path_tabela = f"{gold_path}/{tb_destino}"

# Extrair registros únicos
dim_geografia_df = df_silver.select(
    "Cidade",
    "Estado",
    "Regiao",
    "Distrito",
    "Pais",
    "CodigoPostal"
).dropDuplicates()

# Adicionar data de atualização
dim_geografia_df = dim_geografia_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Verifica se a tabela já existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_geografia_df.alias("source"),
            "target.CodigoPostal = source.CodigoPostal"
        )
        .whenMatchedUpdate(
            set={
                "Cidade": "source.Cidade",
                "Estado": "source.Estado",
                "Regiao": "source.Regiao",
                "Distrito": "source.Distrito",
                "Pais": "source.Pais",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "Cidade": "source.Cidade",
                "Estado": "source.Estado",
                "Regiao": "source.Regiao",
                "Distrito": "source.Distrito",
                "Pais": "source.Pais",
                "CodigoPostal": "source.CodigoPostal",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_geografia_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

#display(spark.read.format("delta").load(path_tabela))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Cliente

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# Nome da tabela destino
tb_destino = "dim_cliente"
path_tabela = f"{gold_path}/{tb_destino}"

# Passo 1 - Extrair clientes únicos
dim_cliente_df = df_silver.select(
    "IDCliente",
    "Nome",
    "Email",
    "Cidade",
    "Estado",
    "Regiao",
    "Distrito",
    "Pais",
    "CodigoPostal"
).dropDuplicates()

# Passo 2 - Join com DimGeografia para obter SK
dim_cliente_com_sk_df = dim_cliente_df.alias("cliente") \
    .join(dim_geografia_df.alias("geografia"),
          (col("cliente.Cidade") == col("geografia.Cidade")) &
          (col("cliente.Estado") == col("geografia.Estado")) &
          (col("cliente.Regiao") == col("geografia.Regiao")) &
          (col("cliente.Distrito") == col("geografia.Distrito")) &
          (col("cliente.Pais") == col("geografia.Pais")) &
          (col("cliente.CodigoPostal") == col("geografia.CodigoPostal")),
          "left") \
    .select(
        "cliente.IDCliente",
        "cliente.Nome",
        "cliente.Email",
        "geografia.sk_geografia"
    )

# Passo 3 - Adicionar data atualização
dim_cliente_com_sk_df = dim_cliente_com_sk_df.withColumn(
    "data_atualizacao",
    current_timestamp()
)

# Passo 4 - Verificar se a tabela existe
if DeltaTable.isDeltaTable(spark, path_tabela):

    delta_table = DeltaTable.forPath(spark, path_tabela)

    (
        delta_table.alias("target")
        .merge(
            dim_cliente_com_sk_df.alias("source"),
            "target.IDCliente = source.IDCliente"
        )
        .whenMatchedUpdate(
            set={
                "Nome": "source.Nome",
                "Email": "source.Email",
                "sk_geografia": "source.sk_geografia",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .whenNotMatchedInsert(
            values={
                "IDCliente": "source.IDCliente",
                "Nome": "source.Nome",
                "Email": "source.Email",
                "sk_geografia": "source.sk_geografia",
                "data_atualizacao": "source.data_atualizacao"
            }
        )
        .execute()
    )

else:

    dim_cliente_com_sk_df.write.format("delta") \
        .mode("overwrite") \
        .save(path_tabela)

display(spark.read.format("delta").load(path_tabela))


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
        col("s.TotalVendas"),
        current_timestamp().alias("data_atualizacao")
    )

# Escrever tabela Fato no formato Delta, particionando por DataVenda (ano e mês)
fato_vendas_df.withColumn("Ano", year("DataVenda")) \
             .withColumn("Mes", month("DataVenda")) \
             .write.format("delta") \
             .mode("append")\
             .option("mergeSchema", "true")\
             .option("MaxRecordsPerFile", 1000000)\
             .partitionBy("Ano", "Mes")\
             .save(f"{gold_path}/{tb_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstração de informação total vendas por ano

# COMMAND ----------

from pyspark.sql.functions import sum, col
gold_path = "/mnt/lhdw/gold/vendas_delta/"
# Consulta da fato vendas por categoria ano a ano com a soma de total de vendas

resultado = spark.read.format("delta").load(f"{gold_path}/fato_vendas") \
    .groupBy("Ano") \
    .agg(sum("TotalVendas").alias("SomaTotalVendas")) \
    .orderBy(col("Ano"), col("SomaTotalVendas").desc())

display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza de Memória

# COMMAND ----------

import gc
# Coletar lixo após operações pesadas para liberar memória
gc.collect()