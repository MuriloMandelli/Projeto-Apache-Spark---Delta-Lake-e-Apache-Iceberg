# Apache Spark com Delta Lake e Apache Iceberg

![Python](https://img.shields.io/badge/python-3.12-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/pyspark-3.5.9-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/delta--spark-3.3.3-00ADD4)
![Iceberg](https://img.shields.io/badge/iceberg-1.10.0-4E8EE9)
![uv](https://img.shields.io/badge/gerenciador-uv-DE5FE9)

Trabalho de pesquisa da disciplina de **Engenharia de Dados** (Engenharia de Software — SATC).
O projeto cria um ambiente local de **PySpark + JupyterLab** e demonstra, em dois notebooks,
os comandos **INSERT**, **UPDATE**, **DELETE** (e MERGE / *time travel*) em tabelas
**Delta Lake** e **Apache Iceberg**, usando o cenário de uma seguradora de veículos.

📘 **Documentação (MkDocs):** https://murilomandelli.github.io/Projeto-Apache-Spark---Delta-Lake-e-Apache-Iceberg/

👥 **Equipe:** Murilo Mandelli · Augusto Benedetti

---

## Sumário

- [Aviso](#aviso)
- [Pré-requisitos](#pré-requisitos)
- [Instalação passo a passo](#instalação-passo-a-passo)
- [Como usar](#como-usar)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Testes e qualidade de código](#testes-e-qualidade-de-código)
- [Documentação (MkDocs)](#documentação-mkdocs)
- [Solução de problemas](#solução-de-problemas)
- [Referências](#referências)

## Aviso

> [!IMPORTANT]
> - Use **Linux** — de preferência o **WSL 2 com Ubuntu 24.04** no Windows 11. O Spark no Windows
>   "puro" exige `winutils.exe` e outras configurações que **não** são cobertas aqui.
> - Na **primeira execução** de cada notebook o Spark baixa JARs do Maven Central
>   (Delta ≈ 6 MB, Iceberg ≈ 45 MB). É necessário ter **internet**.
> - Os notebooks **apagam e recriam** a pasta `data/lakehouse/` a cada execução, para que a
>   demonstração sempre comece do zero.

## Pré-requisitos

| Ferramenta | Versão testada | Para quê |
|---|---|---|
| Windows 11 + WSL 2 | Ubuntu 24.04 LTS | Sistema operacional |
| Java (OpenJDK) | 17 | O Spark roda na JVM |
| Python | 3.12 | Linguagem do projeto (instalado pelo uv se faltar) |
| [uv](https://docs.astral.sh/uv/) | 0.12 | Gerenciador de projeto, pacotes e ambiente virtual |
| Git | 2.x | Versionamento |
| VS Code *(opcional)* | — | Extensões **WSL** e **Jupyter** |

Bibliotecas Python (declaradas no [`pyproject.toml`](pyproject.toml) e travadas no `uv.lock`):

| Pacote | Versão | Grupo |
|---|---|---|
| `pyspark` | 3.5.9 | principal |
| `delta-spark` | 3.3.3 | principal |
| `jupyterlab` | ≥ 4.6 | principal |
| `ipykernel` | ≥ 7.4 | principal |
| `mkdocs-material` | ≥ 9.7 | dev |
| `pytest` | ≥ 9.1 | dev |
| `ruff` | ≥ 0.16 | dev |
| `taskipy` | ≥ 1.14 | dev |

JARs baixados automaticamente pelo Spark:

| JAR | Versão |
|---|---|
| `io.delta:delta-spark_2.12` | 3.3.3 |
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` | 1.10.0 |

## Instalação passo a passo

Todos os comandos abaixo são executados **no terminal do Ubuntu (WSL)**.

### 1. Instalar o WSL com Ubuntu *(se ainda não tiver)*

No **PowerShell do Windows** (como administrador):

```powershell
wsl --install -d Ubuntu-24.04
```

Reinicie o computador, abra o **Ubuntu** pelo menu Iniciar e crie seu usuário.

### 2. Atualizar o sistema e instalar Java 17, Git e curl

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-17-jdk-headless git curl
```

Confira:

```bash
java -version
```

A saída deve começar com `openjdk version "17...`.

### 3. Instalar o uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

### 4. Clonar o repositório

```bash
git clone https://github.com/MuriloMandelli/Projeto-Apache-Spark---Delta-Lake-e-Apache-Iceberg.git
cd Projeto-Apache-Spark---Delta-Lake-e-Apache-Iceberg
```

> [!TIP]
> Clone dentro do sistema de arquivos do Linux (ex.: `~/`), **não** em `/mnt/c/...`.
> O acesso ao disco do Windows pelo WSL é bem mais lento.

### 5. Criar o ambiente virtual e instalar as dependências

```bash
uv sync
```

Esse único comando:

1. instala o Python 3.12 (lido do arquivo `.python-version`) se ele não existir;
2. cria o ambiente virtual em `.venv/`;
3. instala exatamente as versões travadas no `uv.lock` (incluindo o grupo `dev`);
4. instala o pacote do projeto (`src/spark_delta_iceberg`) em modo editável.

### 6. Validar a instalação

```bash
uv run task test
```

Todos os testes devem passar (`passed`).

## Como usar

### Opção A — JupyterLab no navegador

```bash
uv run task lab
```

Abra o link `http://localhost:8888/lab?token=...` exibido no terminal e execute os notebooks em
ordem, com **Run ▸ Run All Cells**:

1. `01_spark_delta_lake.ipynb`
2. `02_spark_apache_iceberg.ipynb`

### Opção B — VS Code

1. Instale as extensões **WSL** e **Jupyter** no VS Code.
2. No terminal do Ubuntu, dentro da pasta do projeto:

   ```bash
   code .
   ```

3. Abra um notebook em `notebooks/`, clique em **Select Kernel** (canto superior direito) →
   **Python Environments** → `.venv (Python 3.12)`.
4. Clique em **Run All**.

### Opção C — executar os notebooks pelo terminal

Executa os dois notebooks e grava as saídas neles:

```bash
uv run task notebooks
```

### Atalhos disponíveis (taskipy)

| Comando | O que faz |
|---|---|
| `uv run task lab` | Abre o JupyterLab na pasta `notebooks/` |
| `uv run task notebooks` | Executa os dois notebooks e salva as saídas |
| `uv run task dados` | Regera os CSV de `data/raw/` a partir de `data/source/script_relacional.sql` |
| `uv run task test` | Roda os testes com pytest |
| `uv run task lint` | Verifica estilo com ruff |
| `uv run task format` | Corrige estilo com ruff |
| `uv run task docs` | Sobe a documentação em http://127.0.0.1:8000 |
| `uv run task deploy` | Publica a documentação no GitHub Pages |

## Estrutura do projeto

```text
spark-delta-iceberg/
├── README.md                  ← você está aqui (instalação e uso)
├── pyproject.toml             ← dependências, tasks, config do ruff/pytest
├── uv.lock                    ← versões exatas de todas as dependências
├── .python-version            ← 3.12
├── mkdocs.yml                 ← configuração do site de documentação
├── assets/
│   └── modelo_er.svg          ← diagrama ER usado nos notebooks
├── data/
│   ├── source/script_relacional.sql  ← dataset original (lab de Modelagem Dimensional)
│   ├── raw/*.csv              ← dados convertidos (cliente, carro, apolice, sinistro)
│   └── lakehouse/             ← tabelas Delta/Iceberg geradas (ignorado pelo Git)
├── docs/                      ← páginas do MkDocs
├── notebooks/
│   ├── 01_spark_delta_lake.ipynb
│   └── 02_spark_apache_iceberg.ipynb
├── scripts/
│   └── converter_sql_para_csv.py
├── src/spark_delta_iceberg/
│   ├── config.py              ← caminhos e versões dos JARs
│   ├── dados.py               ← schemas e leitura dos CSV
│   └── spark_session.py       ← SparkSession para Delta e para Iceberg
└── tests/
```

## Testes e qualidade de código

```bash
uv run task test
```

```bash
uv run task lint
```

Os testes validam o conversor SQL → CSV e se os CSV batem com os schemas usados pelo Spark.

## Documentação (MkDocs)

A documentação explica os conceitos de Spark, Delta Lake e Iceberg e mostra os comandos com seus
resultados. Para ver localmente:

```bash
uv run task docs
```

Para publicar no GitHub Pages (branch `gh-pages`):

```bash
uv run task deploy
```

Na primeira publicação, acesse no GitHub **Settings ▸ Pages** e confira se a origem é a branch
`gh-pages` / pasta `/ (root)`.

## Solução de problemas

<details>
<summary><b><code>JAVA_HOME is not set</code> / <code>Java gateway process exited</code></b></summary>

O Java não está instalado ou não foi encontrado:

```bash
sudo apt install -y openjdk-17-jdk-headless
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

</details>

<details>
<summary><b>Demora ou erro ao baixar os JARs na primeira execução</b></summary>

O Spark baixa os JARs do Maven Central para `~/.ivy2/`. Verifique a internet e execute de novo.
Se o download ficou corrompido:

```bash
rm -rf ~/.ivy2/cache/io.delta ~/.ivy2/cache/org.apache.iceberg ~/.ivy2/jars
```

</details>

<details>
<summary><b><code>DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION</code> ou tabela já existe</b></summary>

Sobrou uma pasta de uma execução interrompida. Apague o lakehouse e rode o notebook do início:

```bash
rm -rf data/lakehouse
```

</details>

<details>
<summary><b>O VS Code não mostra o kernel <code>.venv</code></b></summary>

Confirme que o VS Code foi aberto **pelo WSL** (`code .` no terminal do Ubuntu — no canto inferior
esquerdo deve aparecer `WSL: Ubuntu`) e que você já rodou `uv sync`.

</details>

<details>
<summary><b>Aviso <code>Your hostname ... resolves to a loopback address</code></b></summary>

É só um aviso do Spark no WSL e pode ser ignorado. Para silenciá-lo:

```bash
export SPARK_LOCAL_IP=127.0.0.1
```

</details>

## Referências

**Documentação oficial**

- [Apache Spark 3.5 — Documentação](https://spark.apache.org/docs/3.5.6/) e [PySpark API Reference](https://spark.apache.org/docs/3.5.6/api/python/index.html)
- [Delta Lake — Quickstart](https://docs.delta.io/latest/quick-start.html)
- [Delta Lake — Table deletes, updates and merges](https://docs.delta.io/latest/delta-update.html)
- [Delta Lake — Table utility commands (history, restore, vacuum)](https://docs.delta.io/latest/delta-utility.html)
- [Apache Iceberg — Spark Quickstart](https://iceberg.apache.org/spark-quickstart/)
- [Apache Iceberg — Spark Writes (UPDATE/DELETE/MERGE)](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Apache Iceberg — Spark Queries e tabelas de metadados](https://iceberg.apache.org/docs/latest/spark-queries/)
- [Apache Iceberg — Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Apache Iceberg — Especificação da tabela](https://iceberg.apache.org/spec/)

**Ferramentas**

- [uv](https://docs.astral.sh/uv/) · [JupyterLab](https://jupyterlab.readthedocs.io/) · [MkDocs](https://www.mkdocs.org/) · [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) · [WSL](https://learn.microsoft.com/pt-br/windows/wsl/install)

**Material da disciplina**

- [jlsilva01/spark-delta](https://github.com/jlsilva01/spark-delta) e [jlsilva01/spark-iceberg](https://github.com/jlsilva01/spark-iceberg) — projetos de referência do professor
- Canal **DataWay BR** no YouTube
- *Python para Engenharia de Dados — material de apoio* (padrão de projeto, uv, MkDocs, taskipy)
- Laboratório de Modelagem Dimensional (`script_relacional.sql`) — origem do dataset

**Uso de IA:** conforme orientação do professor, usamos IA generativa (Claude) como apoio na pesquisa, no código e na documentação. Todo o código foi executado e validado no ambiente descrito acima.

---

Projeto acadêmico — SATC · Engenharia de Dados · Prof. Jorge Luiz da Silva
