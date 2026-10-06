"""Caminhos e versões usados em todo o projeto."""

from pathlib import Path

# Raiz do repositório (src/spark_delta_iceberg/config.py -> ../../)
RAIZ_PROJETO = Path(__file__).resolve().parents[2]

# Dados de entrada (CSV gerados a partir do script do laboratório)
PASTA_DADOS_RAW = RAIZ_PROJETO / "data" / "raw"

# Onde as tabelas Delta e Iceberg são gravadas (ignorado pelo Git)
PASTA_LAKEHOUSE = RAIZ_PROJETO / "data" / "lakehouse"
PASTA_DELTA = PASTA_LAKEHOUSE / "delta"
PASTA_ICEBERG = PASTA_LAKEHOUSE / "iceberg"

# Versões dos JARs — precisam ser compatíveis com o PySpark 3.5 (Scala 2.12)
VERSAO_ICEBERG = "1.10.0"
PACOTE_ICEBERG = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{VERSAO_ICEBERG}"

# Nome do catálogo Iceberg usado nos comandos SQL (ex.: iceberg.seguradora.cliente)
CATALOGO_ICEBERG = "iceberg"
