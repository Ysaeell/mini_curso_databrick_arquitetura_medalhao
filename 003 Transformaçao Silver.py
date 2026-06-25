# Databricks notebook source
# MAGIC %md
# MAGIC ### Camada Silver: Limpeza e Transformação
# MAGIC
# MAGIC Aplicar transformações e desnormalizar os dados na camada Silver. Use particionamento para melhorar o desempenho de leitura e escrita.

# COMMAND ----------

# Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Iniciar a SparkSession com configurações otimizadas
spark = SparkSession.builder \
    .appName("Transformação Data Silver") \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Define um número fixo de partições para shuffle, melhorando o paralelismo                 
# Define o tamanho máximo de partições para evitar muitos arquivos pequenos        
# Usa o codec Snappy para compressão rápida, otimizando tempo de leitura e escrita    
# Habilita otimizações adaptativas, ajustando o número de partições dinamicamente com base no tamanho dos dados

# Definir caminhos de armazenamento no Data Lake
# Ler dados na Bronze e Armazenar Silver

bronze_path = "/Volumes/workspace/lhdw/vendas_volume/bronze/vendas"
silver_path = "/Volumes/workspace/lhdw/vendas_volume/silver/vendas"



# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dados da camada Bronze para transformação na camada Silver

# COMMAND ----------

# Ler dados da camada Bronze
df_bronze = spark.read.format("parquet").load(bronze_path)
#display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza dos dados (Cleaning Data)
# MAGIC A limpeza de dados é um processo crucial para garantir a qualidade dos dados. Isso envolve a remoção de dados duplicados ou incorretos, a padronização de formatos e valores de dados e o enriquecimento de dados com informações adicionais. Além disso, é importante verificar e corrigir problemas de qualidade, como erros e inconsistências, para garantir que os dados sejam precisos e confiáveis.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import round

# Realizar transformações necessárias, incluindo a manipulação do campo EmailNome IdCampanha
df_silver = df_bronze.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd")) \
                     .withColumn("Email", lower(expr("regexp_replace(split(EmailNome, ':')[0], '[()]', '')"))) \
                     .withColumn("Nome", expr("split(split(EmailNome, ':')[1], ', ')")) \
                     .withColumn("Nome", expr("concat(Nome[1], ' ', Nome[0])")) \
                     .withColumn("Cidade", expr("split(Cidade, ',')[0]")) \
                     .withColumn("PrecoUnitario", round(col("PrecoUnitario"), 2)) \
                     .withColumn("CustoUnitario", round(col("CustoUnitario"), 2)) \
                     .withColumn("TotalVendas", round(col("PrecoUnitario") * col("Unidades"),2))\
                     .drop("EmailNome")\
                     .drop("IdCampanha")   
                     

#display(df_silver)




# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravar transformações Silver
# MAGIC
# MAGIC Particionamento por ano e mês para otimizar consultas baseadas em data, com recomendação de tamanho de arquivo em formato Parquet

# COMMAND ----------

# Particionamento por ano e mês para otimizar consultas baseadas em data, com recomendação de tamanho de arquivo

df_silver.withColumn("Ano", year("Data")) \
         .withColumn("Mes", month("Data")) \
         .write.option("maxRecordsPerFile", 50000) \
         .partitionBy("Ano", "Mes") \
         .format("parquet") \
         .mode("overwrite") \
         .save(silver_path)
#Contagem de registros
df_silver.count()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Justificativa para particionamento:**
# MAGIC
# MAGIC partitionBy("Ano", "Mes"): Particionar os dados pelas coluna Ano e Mês ajuda a otimizar a leitura quando queremos filtrar ou consultar dados baseados em periodos específicos. Isso reduz o número de arquivos escaneados em consultas, melhorando a performance.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando a Memória

# COMMAND ----------

import gc
gc.collect()

# COMMAND ----------

del df_bronze
del df_silver