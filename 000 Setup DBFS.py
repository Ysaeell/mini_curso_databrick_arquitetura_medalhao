# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Criar os diretórios (DBFS)

# COMMAND ----------

#Nota: Você precisa ter criado o Volume vendas_volume antes na interface do Databricks
# Defina o catálogo e o esquema (database) onde os volumes residem
catalog = "workspace"
schema = "lhdw"

# Os caminhos agora seguem o padrão: /Volumes/<catalog>/<schema>/<volume>/...
base_path = f"/Volumes/{catalog}/{schema}/vendas_volume"

# Criando os diretórios dentro do Volume
dbutils.fs.mkdirs(f"{base_path}/landingzone/vendas/processar")
dbutils.fs.mkdirs(f"{base_path}/landingzone/vendas/processado")
dbutils.fs.mkdirs(f"{base_path}/bronze")
dbutils.fs.mkdirs(f"{base_path}/silver")
dbutils.fs.mkdirs(f"{base_path}/gold")

print("Diretórios criados com sucesso via Unity Catalog Volumes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Resumo das Diferenças
# MAGIC #####Criar um Diretório: 
# MAGIC   Simplesmente cria uma nova pasta no DBFS para organizar seus dados internos.
# MAGIC
# MAGIC #####Montar um Diretório: 
# MAGIC Conecta um armazenamento de objetos externo ao DBFS, permitindo acesso e manipulação de dados externos como se estivessem localmente no Databricks.
# MAGIC
# MAGIC Documentação de apoio
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/files/#work-with-files-in-dbfs-mounts-and-dbfs-root

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios (DBFS)

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt (DBFS)

# COMMAND ----------

# MAGIC %fs ls /Volumes/Workspace/lhdw/vendas_volume

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt/lhdw (DBFS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt/lhdw/landingzone (DBFS)

# COMMAND ----------

# MAGIC %fs ls /Volumes/Workspace/lhdw/vendas_volume/landingzone

# COMMAND ----------

# MAGIC %md
# MAGIC ####Conhecendo os diretórios /FileStore/ (DBFS)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Conhecendo os diretórios /databricks-datasets/ (DBFS)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apagando pastas/diretorios
# MAGIC Processar apenas se necessario

# COMMAND ----------

# Deletando os diretórios dentro do Volume
#dbutils.fs.rm(f"/Volumes/Workspace/lhdw/vendas_volume/landingzone", recurse=True)
#dbutils.fs.rm(f"/Volumes/Workspace/lhdw/vendas_volume/bronze", recurse=True)
#dbutils.fs.rm(f"/Volumes/Workspace/lhdw/vendas_volume/silver", recurse=True)
#dbutils.fs.rm(f"/Volumes/Workspace/lhdw/vendas_volume/gold", recurse=True)
