"""Converte o script SQL Server do laboratório (script_relacional.sql) em arquivos CSV.

O script original foi disponibilizado na disciplina (lab de Modelagem Dimensional) e
contém o modelo relacional de uma seguradora de veículos com comandos
``INSERT [relacional].[tabela] (...) VALUES (...)`` no dialeto T-SQL.

Uso:
    uv run python scripts/converter_sql_para_csv.py data/source/script_relacional.sql data/raw
"""

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

# Tabelas do modelo relacional usadas no trabalho
TABELAS = ("cliente", "carro", "apolice", "sinistro")

PADRAO_INSERT = re.compile(
    r"^INSERT \[relacional\]\.\[(?P<tabela>\w+)\] "
    r"\((?P<colunas>[^)]*)\) VALUES \((?P<valores>.*)\)\s*$"
)
PADRAO_CAST = re.compile(r"^CAST\((?P<valor>.+) AS \w+(\(\d+, ?\d+\))?\)$")


def separar_valores(texto: str) -> list[str]:
    """Separa a lista de VALUES respeitando aspas simples e parênteses."""
    valores, atual, profundidade, em_texto = [], [], 0, False
    i = 0
    while i < len(texto):
        c = texto[i]
        if em_texto:
            atual.append(c)
            if c == "'":
                # '' é um apóstrofo escapado dentro do texto
                if i + 1 < len(texto) and texto[i + 1] == "'":
                    atual.append("'")
                    i += 1
                else:
                    em_texto = False
        elif c == "'":
            em_texto = True
            atual.append(c)
        elif c == "(":
            profundidade += 1
            atual.append(c)
        elif c == ")":
            profundidade -= 1
            atual.append(c)
        elif c == "," and profundidade == 0:
            valores.append("".join(atual).strip())
            atual = []
        else:
            atual.append(c)
        i += 1
    valores.append("".join(atual).strip())
    return valores


def normalizar_valor(valor: str) -> str | None:
    """Converte um literal T-SQL para texto simples (None para NULL)."""
    if valor.upper() == "NULL":
        return None
    cast = PADRAO_CAST.match(valor)
    if cast:
        valor = cast.group("valor").strip()
    if valor.startswith("N'"):
        valor = valor[1:]
    if valor.startswith("'") and valor.endswith("'"):
        valor = valor[1:-1].replace("''", "'").strip()
        # datas no formato 2024-03-15T15:00:22.000 -> 2024-03-15 15:00:22
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}", valor):
            valor = valor.replace("T", " ")[:19]
    return valor


def converter_linha(linha: str) -> tuple[str, list[str], list[str | None]] | None:
    """Retorna (tabela, colunas, valores) de uma linha INSERT ou None se não for INSERT."""
    m = PADRAO_INSERT.match(linha.strip())
    if not m:
        return None
    colunas = [c.strip(" []") for c in m.group("colunas").split(",")]
    valores = [normalizar_valor(v) for v in separar_valores(m.group("valores"))]
    return m.group("tabela"), colunas, valores


def main(origem: Path, destino: Path) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    cabecalhos: dict[str, list[str]] = {}
    linhas: dict[str, list[list[str | None]]] = defaultdict(list)

    with origem.open(encoding="utf-8", errors="replace") as arquivo:
        for linha in arquivo:
            resultado = converter_linha(linha)
            if resultado is None:
                continue
            tabela, colunas, valores = resultado
            if tabela not in TABELAS:
                continue
            cabecalhos.setdefault(tabela, colunas)
            linhas[tabela].append(valores)

    for tabela in TABELAS:
        caminho = destino / f"{tabela}.csv"
        with caminho.open("w", newline="", encoding="utf-8") as saida:
            escritor = csv.writer(saida)
            escritor.writerow(cabecalhos[tabela])
            escritor.writerows(linhas[tabela])
        print(f"{tabela:<10} {len(linhas[tabela]):>6} linhas -> {caminho}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
