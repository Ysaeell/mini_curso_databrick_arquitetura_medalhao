# Pipeline de Engenharia de Dados com Databricks e Delta Lake (Arquitetura Medallion)

Este repositório contém um projeto *end-to-end* de Engenharia de Dados construído no **Databricks** utilizando **PySpark** e **Delta Lake**. O objetivo central é demonstrar a implementação da **Arquitetura Medallion** (Bronze, Silver e Gold) para a construção de um Data Lakehouse escalável, otimizado e confiável.

## 🎯 Objetivo do Projeto
Desenvolver um pipeline completo de ingestão, processamento, modelagem dimensional e manutenção de dados de vendas. O projeto aborda desde a extração dos dados brutos até a disponibilização em um formato otimizado para análises e relatórios de BI, aplicando boas práticas de performance e governança de dados.

## 🛠️ Tecnologias Utilizadas
* **Databricks:** Ambiente de processamento distribuído.
* **PySpark:** Motor principal para processamento e transformação de grandes volumes de dados.
* **Delta Lake:** Camada de armazenamento open-source que traz confiabilidade (transações ACID) aos data lakes.
* **Python:** Linguagem de programação suporte.
* **Databricks File System (DBFS) / Unity Catalog Volumes:** Gerenciamento de armazenamento no Data Lake.

## 🏗️ Arquitetura Medallion
O projeto foi estruturado utilizando o paradigma de três camadas:

1. **Camada Bronze (Raw):** Armazena os dados brutos recém-ingestados no formato Parquet, particionados por Ano e Mês, mantendo o histórico original sem transformações.
2. **Camada Silver (Cleaned/Conformed):** Responsável pela limpeza, padronização, filtragem e tratamento de dados. Aqui os dados ganham estrutura (como tipagem correta e cálculos preliminares) para facilitar análises futuras.
3. **Camada Gold (Curated/Business-level):** Dados modelados de acordo com as regras de negócio em um **Star Schema** (Tabelas Fato e Dimensões), utilizando recursos do Delta Lake, chaves substitutas (*Surrogate Keys*) e otimizações de leitura.

---

## 📂 Estrutura do Projeto (Notebooks)

* **`000 Setup DBFS.py`:** Configuração inicial de diretórios essenciais (`landingzone`, `bronze`, `silver`, `gold`) no Unity Catalog/Volumes do Databricks.
* **`001 Importando arquivos.py`:** Faz o download automatizado via Python (usando `requests`) de arquivos CSV no GitHub diretamente para a *Landing Zone* do Data Lake.
* **`002 Load Bronze.py`:** Lê os dados em CSV, aplica um *schema* inicial estruturado e salva os dados no formato Parquet particionado, movendo os arquivos originais para uma pasta de processados.
* **`003 Transformação Silver.py`:** Realiza operações de *data cleaning*, enriquecimento (como quebra de colunas e arredondamentos), e grava os resultados em Parquet com particionamento e limitação do número de registros por arquivo.
* **`004 Load Gold Delta.py`:** Constrói a modelagem dimensional (Tabela Fato e Tabelas Dimensão: Produto, Categoria, Segmento, Fabricante, Geografia, Cliente) gerando chaves *Surrogate Keys* (`monotonically_increasing_id`) e salva em formato **Delta**.
* **`005 Load Gold Delta Incremental.py`:** Implementa a lógica de **SCD (Slowly Changing Dimensions)** para atualização incremental dos dados. Utiliza comandos `MERGE` do Delta Lake (`whenMatchedUpdate` e `whenNotMatchedInsert`) para atualizar as tabelas Gold sem sobreescrever toda a base.
* **`006 Consultas Otimizadas.py`:** Apresenta técnicas de otimização no PySpark, incluindo *Predicate Pushdown* (leitura com filtros nativos) e *Broadcast Joins* para tabelas de dimensão, reduzindo o tempo de *shuffle* nos clusters.
* **`007 Criação de tabelas Delta.py`:** Cria o banco de dados/schema (`lhdw_vendas`) e registra as tabelas Delta construídas nos notebooks anteriores no Hive Metastore/Unity Catalog, facilitando as consultas via SQL.
* **`008 Rotinas de Manutenção Delta.py`:** Demonstra operações críticas para a saúde do Delta Lake:
  * **Vacuum:** Remoção de arquivos históricos expirados.
  * **Optimize & Z-Ordering:** Compactação de arquivos pequenos e ordenação multidimensional para melhora de tempo de leitura.
  * **Time Travel / History:** Demonstração de consulta de versões passadas e restauração (`restoreToVersion`).
  * **Compaction / Repartition / Coalesce:** Exemplos de balanceamento de partições.

## 🚀 Boas Práticas Adotadas
* **Gerenciamento de Memória:** Uso explícito do `gc.collect()`, limpeza de cache, e exclusão de variáveis grandes (`del`) para evitar problemas de limite de memória em DataFrames massivos.
* **Configurações Avançadas do Spark:** Habilitação do *Adaptive Query Execution (AQE)*, definição do codec de compressão *Snappy* e fixação do *shuffle partitions* (`spark.sql.shuffle.partitions`).
* **SCD (UPSERT):** Aplicação prática de *Slowly Changing Dimensions* evitando *full loads* caros e garantindo atualização das tabelas baseadas em *primary keys* e carimbos de data/hora atual (`current_timestamp`).

## 💡 Como Executar
1. Clone este repositório no seu ambiente (local ou diretamente na plataforma Databricks).
2. Certifique-se de configurar um Cluster Spark no Databricks compatível com a biblioteca Delta Lake.
3. Execute os notebooks seguindo a numeração sequencial (do `000` ao `008`), garantindo a criação da infraestrutura antes do processamento dos dados.

---
*Desenvolvido como portfólio para demonstração de conhecimentos avançados em Engenharia de Dados.*
