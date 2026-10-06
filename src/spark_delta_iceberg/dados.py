"""Schemas e leitura dos CSV do cenário da seguradora."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_delta_iceberg.config import PASTA_DADOS_RAW

# Schema explícito de cada tabela: evita inferência errada (ex.: CPF virar número)
SCHEMAS: dict[str, StructType] = {
    "cliente": StructType(
        [
            StructField("cd_cliente", IntegerType(), False),
            StructField("nome", StringType(), True),
            StructField("cpf", StringType(), False),
            StructField("sexo", StringType(), True),
            StructField("dt_nascimento", DateType(), True),
        ]
    ),
    "carro": StructType(
        [
            StructField("placa", StringType(), False),
            StructField("cd_modelo", IntegerType(), True),
            StructField("chassi", StringType(), False),
            StructField("ano", IntegerType(), True),
            StructField("cor", StringType(), True),
        ]
    ),
    "apolice": StructType(
        [
            StructField("cd_apolice", IntegerType(), False),
            StructField("cd_cliente", IntegerType(), False),
            StructField("dt_inicio_vigencia", TimestampType(), True),
            StructField("dt_fim_vigencia", TimestampType(), True),
            StructField("vl_cobertura", DecimalType(10, 2), True),
            StructField("vl_franquia", DecimalType(10, 2), True),
            StructField("placa", StringType(), True),
        ]
    ),
    "sinistro": StructType(
        [
            StructField("cd_sinistro", IntegerType(), False),
            StructField("placa", StringType(), False),
            StructField("dt_sinistro", TimestampType(), True),
            StructField("local_sinistro", IntegerType(), True),
            StructField("condutor", StringType(), True),
        ]
    ),
}

TABELAS: tuple[str, ...] = tuple(SCHEMAS)


def ler_csv(spark: SparkSession, tabela: str) -> DataFrame:
    """Lê ``data/raw/<tabela>.csv`` aplicando o schema definido em ``SCHEMAS``."""
    return (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(SCHEMAS[tabela])
        .csv(str(PASTA_DADOS_RAW / f"{tabela}.csv"))
    )
