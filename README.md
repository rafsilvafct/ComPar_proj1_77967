# Projeto 1 de Computação Paralela

Este repositório contém a implementação de um pipeline de processamento e análise de dados em grande escala utilizando PySpark, desenvolvido no âmbito da unidade curricular de Computação Paralela e Distribuída (2026/2027).

O projeto realiza a limpeza, agregação temporal, estatísticas por zona e análises avançadas sobre os dados de viagens dos táxis amarelos de Nova Iorque (*NYC TLC Yellow Taxi Trip Records*).

---

## 1. Dataset & Requisitos Prévios

### Fonte dos Dados
* **Origem:** NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records
* **Website Oficial:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* **Ano Utilizado:** **2025** (Ficheiros mensais `.parquet` do ano de 2025)
* **Ficheiro Auxiliar:** `taxi_zone_lookup.csv` (Mapeamento de 265 zonas de táxi)

### Estrutura de Pastas Esperada

```text
.
├── data/                       # Mapeado para /data/project no Docker
│   ├── yellow_tripdata_2025-01.parquet
│   ├── yellow_tripdata_2025-02.parquet
│   ├── ...
│   ├── yellow_tripdata_2025-12.parquet
│   └── taxi_zone_lookup.csv
├── docker-spark-env/
│   └── docker-compose.yml
├── src/                        # Mapeado para /apps/project no Docker
│   └── run_project_pipeline.py
├── notebooks/
│   ├── project.ipynb
│   └── spark_profiler.py
├── report/
│   ├── relatorio.pdf
│   └── relatorio.zip
└── README.md
```

---

## 2. Iniciar o Cluster Spark (Docker)

A infraestrutura distribuída é composta por um nó **Spark Master**, nós **Spark Workers** escaláveis e um servidor **Jupyter**.

### Passos de Inicialização:

1. Entra na pasta do ambiente Docker:
   ```bash
   cd docker-spark-env
   ```

2. Inicializa o cluster definindo o número de trabalhadores (*workers*) com a flag `--scale`:
   ```bash
   # Substitui 'x' pelo número pretendido de trabalhadores (ex: 2, 4, etc.)
   docker compose up -d --scale spark-worker=x
   ```
   *Exemplo para subir o cluster com 2 workers:*
   ```bash
   docker compose up -d --scale spark-worker=2
   ```

---

## 3. Execução do Pipeline PySpark

Após os contentores estarem a correr, podes submeter o job ao cluster através da pasta `docker-spark-env`.

### Sintaxe Geral do Comando
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /apps/project/run_project_pipeline.py \
  --data <CAMINHO_INTERNO_DOS_DADOS> \
  --output <CAMINHO_INTERNO_DA_PASTA_DE_OUTPUT>
```

> **Atenção:** O argumento `<CAMINHO_INTERNO_DOS_DADOS>` refere-se ao caminho do volume dentro do contentor onde os dados estão mapeados (por exemplo, `/data/project`).

> **Nota:** Se utilizar-se o valore /apps/project/output para o argumento `<CAMINHO_INTERNO_DA_PASTA_DE_OUTPUT>` a pasta `output` vai aparecer na pasta `src`.

### Exemplo Prático de Execução

Executar o pipeline nos dados completos de 2025 localizados na pasta `/data/project`:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /apps/project/run_project_pipeline.py \
  --data /data/project \
  --output /apps/project/output
```

---

## 4. Argumentos do Pipeline (`run_project_pipeline.py`)

O script principal suporta os seguintes parâmetros por linha de comandos:

| Parâmetro | Descrição | Exemplo de Valor |
| :--- | :--- | :--- |
| `--data` | Diretoria interna com os ficheiros de dados a analisar. | `/data/project` |
| `--output` | Diretoria onde serão gravados os ficheiros Parquet resultantes. | `/apps/project/output` |
---