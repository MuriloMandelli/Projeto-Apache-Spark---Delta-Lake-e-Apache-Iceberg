"""Testes do conversor SQL Server -> CSV."""

import importlib.util
from pathlib import Path

CAMINHO = Path(__file__).resolve().parents[1] / "scripts" / "converter_sql_para_csv.py"
spec = importlib.util.spec_from_file_location("converter", CAMINHO)
converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(converter)


def test_converte_insert_com_cast_e_texto():
    linha = (
        "INSERT [relacional].[apolice] ([cd_apolice], [cd_cliente], [dt_inicio_vigencia], "
        "[dt_fim_vigencia], [vl_cobertura], [vl_franquia], [placa]) VALUES (1, 4795, "
        "CAST(N'2025-02-01T15:25:41.000' AS DateTime), "
        "CAST(N'2025-09-09T15:25:41.000' AS DateTime), "
        "CAST(554382.00 AS Numeric(10, 2)), CAST(77298.00 AS Numeric(10, 2)), N'HEG0631   ')"
    )
    tabela, colunas, valores = converter.converter_linha(linha)

    assert tabela == "apolice"
    assert colunas[0] == "cd_apolice" and colunas[-1] == "placa"
    assert valores == [
        "1",
        "4795",
        "2025-02-01 15:25:41",
        "2025-09-09 15:25:41",
        "554382.00",
        "77298.00",
        "HEG0631",
    ]


def test_apostrofo_escapado_e_null():
    linha = (
        "INSERT [relacional].[cliente] ([cd_cliente], [nome], [cpf], [sexo], [dt_nascimento]) "
        "VALUES (7, N'JOANA D''ARC', N'12345678901', NULL, CAST(N'2000-01-02' AS Date))"
    )
    _, _, valores = converter.converter_linha(linha)
    assert valores == ["7", "JOANA D'ARC", "12345678901", None, "2000-01-02"]


def test_ignora_linhas_que_nao_sao_insert():
    assert converter.converter_linha("GO") is None
    assert converter.converter_linha("CREATE TABLE [relacional].[carro](") is None
