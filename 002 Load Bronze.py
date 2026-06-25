# Databricks notebook source
# MAGIC %md
# MAGIC **1. Configurações Iniciais e Importações**
# MAGIC
# MAGIC Aqui está um exemplo de um notebook em PySpark para implementar a arquitetura Medallion com as camadas Bronze, Silver e Gold, utilizando Databricks e Delta Lake. Este exemplo segue as boas práticas de desenvolvimento e performance, incluindo a criação de surrogate keys (chaves substitutas) para as dimensões e otimização da tabela de fatos na camada Gold.
# MAGIC
# MAGIC **Explicações:**
# MAGIC
# MAGIC - Importar bibliotecas e funções necessárias.
# MAGIC - Definir os caminhos de arquivo para as camadas Bronze, Silver e Gold.
# MAGIC - Configurar as definições do Spark para um desempenho ótimo, como partições de shuffle automático.

# COMMAND ----------

# Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Iniciar a SparkSession com configurações otimizadas
spark = SparkSession.builder \
    .appName("Load Data Bronze") \
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
lz_path_in = "/Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/"
lz_path_out = "/Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processado"
bronze_path = "/Volumes/workspace/lhdw/vendas_volume/bronze/vendas"



# COMMAND ----------

# MAGIC %md
# MAGIC **Justificativa:**
# MAGIC
# MAGIC - **spark.sql.shuffle.partitions**: Define o número de partições para operações que envolvem shuffle (como joins e agregações). Escolher um valor fixo, como 200, garante que o cluster trabalhe de forma paralela de maneira eficiente.
# MAGIC
# MAGIC Um cálculo comum para o número de partições é o seguinte:
# MAGIC
# MAGIC _`número de partições = número de núcleos de CPU * 2 ou 3`_
# MAGIC
# MAGIC Isso ajuda a garantir que o Spark use todos os núcleos disponíveis.
# MAGIC - **spark.sql.files.maxPartitionByte**s: Definimos o tamanho máximo dos arquivos particionados para evitar a criação de muitos arquivos pequenos, o que prejudicaria a performance de leitura e escrita.
# MAGIC - **spark.sql.parquet.compression.codec**: Snappy é uma escolha comum para Parquet, pois oferece uma boa combinação de compressão rápida e descompressão eficiente.
# MAGIC - **spark.sql.adaptive.enabled**: A otimização adaptativa ajusta o plano de execução conforme o tamanho dos dados, melhorando o desempenho automaticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **2. Camada Bronze: Ingestão de Dados Brutos**
# MAGIC
# MAGIC A camada Bronze armazena dados brutos com formato parquet, sem transformações significativas. Aqui vamos simplesmente gravar os dados brutos como parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um Schema para dados brutos

# COMMAND ----------

# Definir o esquema dos dados brutos
schema_lz = StructType([
    StructField("IDProduto", IntegerType(), True),
    StructField("Data", DateType(), True),
    StructField("IDCliente", IntegerType(), True),
    StructField("IDCampanha", IntegerType(), True),
    StructField("Unidades", IntegerType(), True),
    StructField("Produto", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Segmento", StringType(), True),
    StructField("IDFabricante", IntegerType(), True),
    StructField("Fabricante", StringType(), True),
    StructField("CustoUnitario", DoubleType(), True),
    StructField("PrecoUnitario", DoubleType(), True),
    StructField("CodigoPostal", StringType(), True),
    StructField("EmailNome", StringType(), True),
    StructField("Cidade", StringType(), True),
    StructField("Estado", StringType(), True),
    StructField("Regiao", StringType(), True),
    StructField("Distrito", StringType(), True),
    StructField("Pais", StringType(), True)
])

# Leitura dos dados utilizando a coluna oculta _metadata
df_vendas = (spark.read
             .option("header", "true")
             .schema(schema_lz)
             .csv(lz_path_in)
             # Substituímos input_file_name() por _metadata.file_path
             .withColumn("filename", regexp_extract("_metadata.file_path", "([^/]+)$", 0))
            )

distinct_filenames = df_vendas.select("filename").distinct()

# Exibindo o DataFrame
#display(df_vendas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apresentando os arquivos lidos

# COMMAND ----------


#display(distinct_filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvar/Persistir dados na camada Bronze Bronze
# MAGIC
# MAGIC Os dados serão salvos de forma particionada **Ano e Mês**

# COMMAND ----------

# Escrever a tabela no formato Parquet, particionando por DataVenda (ano e mês)
df_vendas.withColumn("Ano", year("Data")) \
             .withColumn("Mes", month("Data")) \
             .write.mode("overwrite").partitionBy("Ano", "Mes").parquet(bronze_path) #OVERWRITE: Grava por cima

# Apresentando o DataFrame
#display(df_vendas)

# COMMAND ----------

# MAGIC %md
# MAGIC **Justificativas:**
# MAGIC
# MAGIC - Lê os dados brutos a partir de um arquivo CSV na landing zone e escreve esses dados no formato Parquet na camada Bronze.
# MAGIC - O Parquet é escolhido pelo seu suporte a colunas e sua eficiência tanto em termos de espaço quanto em desempenho de leitura e escrita.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover os arquivos processados para pasta processado

# COMMAND ----------

from pyspark.sql import functions as F
# Unpersist the DataFrame to ensure it does not hold onto file references
# distinct_filenames.unpersist() #nao é suportado em serverless
# Mover os arquivos processados para o caminho lz_path_out
# Nota: A operação de mover arquivos diretamente não é suportada pelo DataFrame API do Spark.
#       É necessário utilizar o dbutils.fs.mv para mover os arquivos manualmente após o processamento.


# Primeiro, verifique se há arquivos a serem movidos
if distinct_filenames.select("filename").distinct().count() > 0:
    filenames = distinct_filenames.select("filename").distinct().collect()

    for row in filenames:
        src_path = row.filename
        dbutils.fs.mv(lz_path_in + "/" + src_path, lz_path_out)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidências

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/bronze/vendas/Ano=2013/Mes=10

# COMMAND ----------

# MAGIC %md
# MAGIC ### A opção de gravar dados no modo "append" 
# MAGIC
# MAGIC Permite adicionar novos dados a um arquivo existente, sem substituir ou excluir os dados já presentes. 
# MAGIC
# MAGIC No caso específico do código fornecido, a linha de código comentada `df_vendas.withColumn("Ano", year("Data")) \ .withColumn("Mes", month("Data")) \ .write.mode("append").partitionBy("Ano", "Mes").parquet(bronze_path)` indica que os dados do DataFrame `df_vendas` serão adicionados ao arquivo Parquet existente no caminho `bronze_path`, mantendo a estrutura de particionamento por ano e mês.
# MAGIC
# MAGIC Essa opção é útil quando se deseja adicionar novos dados a um conjunto de dados já existente, como por exemplo, quando novas vendas são registradas e precisam ser incorporadas ao conjunto de dados de vendas existente.

# COMMAND ----------

#df_vendas.withColumn("Ano", year("Data")) \
#         .withColumn("Mes", month("Data")) \
#         .write.mode("append").partitionBy("Ano", "Mes").parquet(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerenciar o uso de memória 
# MAGIC Em PySpark, é importante gerenciar o uso de memória eficientemente, especialmente quando se trabalha com grandes conjuntos de dados. Para isso, você pode usar alguns comandos específicos que ajudam a liberar memória, remover objetos em cache ou persistidos e forçar a coleta de lixo.
# MAGIC
# MAGIC **1. Limpar cache:**
# MAGIC PySpark armazena dados em cache para melhorar o desempenho de operações repetidas. Para liberar esses dados, você pode usar o comando unpersist().

# COMMAND ----------

# Exemplo de como liberar o cache de um DataFrame

#df_vendas.unpersist() #NÃO SUPORTADO PARA SERVERLESS COMPUTE

# O comando unpersist() remove o DataFrame do cache, liberando a memória associada. Ele é especialmente útil quando você já não precisa mais dos dados persistidos.

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Limpar todos os dados em cache:**
# MAGIC
# MAGIC Se houver vários DataFrames em cache, você pode limpá-los todos de uma vez.

# COMMAND ----------

# Limpar todos os dados em cache

#spark.catalog.clearCache() #NÃO SUPORTADO PARA SERVERLESS COMPUTE

# clearCache() limpa o cache de todos os objetos em cache no SparkSession atual, liberando uma quantidade significativa de memória quando múltiplos DataFrames estão sendo reutilizados.

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Forçar coleta de lixo:**
# MAGIC
# MAGIC O Python possui um coletor de lixo que remove objetos não referenciados da memória. Você pode forçar a coleta de lixo para liberar memória.

# COMMAND ----------

import gc
gc.collect()

#Comentário: Esse comando força o coletor de lixo a executar imediatamente, liberando a memória de objetos Python que não estão mais em uso.

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Liberar variáveis manualmente:**
# MAGIC
# MAGIC Se você criou variáveis grandes que não são mais necessárias, você pode removê-las explicitamente.

# COMMAND ----------

del df_vendas

# O comando del remove o objeto da memória. Isso é útil quando você tem grandes DataFrames ou objetos Python que já não são necessários.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Dicas adicionais:**
# MAGIC - Evite cachear DataFrames desnecessários.
# MAGIC
# MAGIC **Resumo**
# MAGIC
# MAGIC - **Para uma limpeza rápida e geral**: Use spark.catalog.clearCache().
# MAGIC - **Para liberar memória de DataFrames específicos**: Use df.unpersist().
# MAGIC - **Para remover variáveis específicas**: Use del.
# MAGIC - **Para uma solução completa**: Reinicie o cluster.