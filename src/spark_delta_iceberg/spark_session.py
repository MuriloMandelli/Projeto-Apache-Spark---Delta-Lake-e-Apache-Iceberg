"""Criação das sessões Spark configuradas para Delta Lake e para Apache Iceberg."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from spark_delta_iceberg.config import (
    CATALOGO_ICEBERG,
    PACOTE_ICEBERG,
    PASTA_DELTA,
    PASTA_ICEBERG,
)


def _builder_base(nome_app: str) -> SparkSession.Builder:
    """Configurações comuns: Spark local, poucos shuffles e logs enxutos."""
    return (
        SparkSession.builder.appName(nome_app)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
    )


def criar_sessao_delta(nome_app: str = "spark-delta-lake") -> SparkSession:
    """Cria uma SparkSession com as extensões do Delta Lake.

    - ``DeltaSparkSessionExtension`` habilita UPDATE, DELETE, MERGE, DESCRIBE HISTORY etc.
    - ``DeltaCatalog`` faz o catálogo padrão do Spark entender tabelas ``USING DELTA``.
    - ``configure_spark_with_delta_pip`` baixa o JAR delta-spark compatível com a
      versão do pacote Python instalado.
    """
    builder = (
        _builder_base(nome_app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(PASTA_DELTA))
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def criar_sessao_iceberg(nome_app: str = "spark-apache-iceberg") -> SparkSession:
    """Cria uma SparkSession com um catálogo Iceberg local do tipo ``hadoop``.

    - ``spark.jars.packages`` baixa o runtime do Iceberg do Maven Central.
    - ``IcebergSparkSessionExtensions`` habilita UPDATE, DELETE, MERGE e procedures (CALL).
    - O catálogo ``iceberg`` grava metadados e dados em ``data/lakehouse/iceberg``.
    """
    spark = (
        _builder_base(nome_app)
        .config("spark.jars.packages", PACOTE_ICEBERG)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{CATALOGO_ICEBERG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOGO_ICEBERG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOGO_ICEBERG}.warehouse", str(PASTA_ICEBERG))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
