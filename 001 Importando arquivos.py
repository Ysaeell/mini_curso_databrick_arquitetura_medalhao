# Databricks notebook source
# MAGIC %md
# MAGIC ###Importar arquivos para DBFS Landingzone
# MAGIC
# MAGIC Para baixar um arquivo do GitHub e salvá-lo no Databricks, você pode seguir os passos abaixo:
# MAGIC

# COMMAND ----------

import requests
import os

# 1. URL do arquivo
url = 'https://github.com/andrerosa77/trn-pyspark/raw/main/dados_2012.csv'

# 2. Caminho do Volume (Verifique se o catálogo é 'workspace' ou 'main')
# O padrão é: /Volumes/<catalogo>/<esquema>/<volume>/path/to/file
destination_path = '/Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/dados_2012.csv'

# Certifique-se de que a pasta de destino existe (usando dbutils para pastas do Volume)
folder_path = '/Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar'
dbutils.fs.mkdirs(folder_path)

# 3. Baixar e gravar diretamente no Volume usando Python padrão
print(f"Iniciando download para: {destination_path}")

response = requests.get(url)

if response.status_code == 200:
    # O Python consegue escrever diretamente em /Volumes/ como se fosse um disco local
    with open(destination_path, 'wb') as f:
        f.write(response.content)
    print(f"Sucesso! Arquivo salvo e disponível em: {destination_path}")
else:
    print(f"Erro ao baixar o arquivo. Status Code: {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Evidência do Arquivo criado

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/")


# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/lhdw/vendas_volume/landingzone/vendas/processar/
# MAGIC