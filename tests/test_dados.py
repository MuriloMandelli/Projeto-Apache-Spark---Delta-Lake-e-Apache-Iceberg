"""Garante que os CSV versionados batem com os schemas usados pelo Spark."""

import csv

import pytest

from spark_delta_iceberg.config import PASTA_DADOS_RAW
from spark_delta_iceberg.dados import SCHEMAS, TABELAS


@pytest.mark.parametrize("tabela", TABELAS)
def test_csv_existe_e_cabecalho_bate_com_schema(tabela):
    caminho = PASTA_DADOS_RAW / f"{tabela}.csv"
    assert caminho.exists(), f"{caminho} não encontrado — rode `uv run task dados`"

    with caminho.open(encoding="utf-8") as arquivo:
        cabecalho = next(csv.reader(arquivo))

    assert cabecalho == SCHEMAS[tabela].fieldNames()


def test_quantidade_de_linhas():
    esperado = {"cliente": 20010, "carro": 10001, "apolice": 10000, "sinistro": 10000}
    for tabela, linhas in esperado.items():
        with (PASTA_DADOS_RAW / f"{tabela}.csv").open(encoding="utf-8") as arquivo:
            assert sum(1 for _ in arquivo) - 1 == linhas
