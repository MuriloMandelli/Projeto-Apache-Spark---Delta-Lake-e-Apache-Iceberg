# Delta Lake x Apache Iceberg

Os dois resolvem o mesmo problema — transformar arquivos Parquet em tabelas com transações,
versões e `UPDATE`/`DELETE` — mas com abordagens diferentes.

| Aspecto | Delta Lake | Apache Iceberg |
|---|---|---|
| Origem | Databricks (2019), Linux Foundation | Netflix (2017), Apache Software Foundation |
| Metadados | Log de transações sequencial (`_delta_log/*.json` + checkpoints) | Árvore: `metadata.json` → manifest list → manifests |
| Nome da versão | **Versão** (0, 1, 2...) | **Snapshot** (id numérico) |
| Catálogo | Usa o catálogo do Spark (`spark_catalog`) | Catálogo próprio e plugável (hadoop, REST, Hive, Glue, Nessie...) |
| Referência à tabela | `seguradora.cliente` | `iceberg.seguradora.cliente` |
| Particionamento | Colunas explícitas (ou *generated columns*); *liquid clustering* | **Hidden partitioning** com transformações e **partition evolution** |
| UPDATE/DELETE | Copy-on-write ou *deletion vectors* | Copy-on-write ou **merge-on-read** (*delete files*), configurável por operação |
| Histórico | `DESCRIBE HISTORY` | Tabelas de metadados (`.snapshots`, `.history`, `.files`...) |
| Time travel | `VERSION AS OF n` / `TIMESTAMP AS OF` | `VERSION AS OF <snapshot_id>` / `TIMESTAMP AS OF` |
| Desfazer | `RESTORE TABLE ... TO VERSION AS OF n` | `CALL system.rollback_to_snapshot(...)` |
| Limpeza | `VACUUM` | `CALL system.expire_snapshots(...)`, `remove_orphan_files` |
| API Python extra | `DeltaTable` (update, delete, merge, history) | Via SQL (ou PyIceberg, fora do Spark) |
| Ecossistema mais forte | Databricks, Spark | Multi-motor: Trino, Flink, Snowflake, Athena, DuckDB |

## Mesmos comandos, lado a lado

=== "Delta Lake"

    ```sql
    INSERT INTO seguradora.carro VALUES ('SAT2026', 10, '9BWZZZ377VT004251', 2024, 'PRATA');
    UPDATE seguradora.carro SET cor = 'PRETO' WHERE placa = 'CXK4218';
    DELETE FROM seguradora.sinistro WHERE cd_sinistro = 10001;
    SELECT * FROM seguradora.carro VERSION AS OF 1;
    RESTORE TABLE seguradora.apolice TO VERSION AS OF 3;
    ```

=== "Apache Iceberg"

    ```sql
    INSERT INTO iceberg.seguradora.carro VALUES ('SAT2026', 10, '9BWZZZ377VT004251', 2024, 'PRATA');
    UPDATE iceberg.seguradora.carro SET cor = 'PRETO' WHERE placa = 'CXK4218';
    DELETE FROM iceberg.seguradora.sinistro WHERE cd_sinistro = 10001;
    SELECT * FROM iceberg.seguradora.carro VERSION AS OF 4394266455667682387;
    CALL iceberg.system.rollback_to_snapshot('seguradora.apolice', 659399091093747861);
    ```

## O que observamos neste trabalho

- **Sintaxe SQL quase idêntica** para INSERT, UPDATE, DELETE e MERGE. A maior diferença está na
  configuração da `SparkSession` e no nome qualificado das tabelas.
- O **Delta** foi mais simples de configurar (o pacote `delta-spark` cuida do JAR) e tem a API
  `DeltaTable` em Python, além de devolver o número de linhas afetadas em cada comando.
- O **Iceberg** se destacou no **hidden partitioning** e na escolha entre **copy-on-write** e
  **merge-on-read**, e expõe muita informação útil nas tabelas de metadados.

## Quando escolher cada um

- **Delta Lake** — ambiente centrado em Spark/Databricks, time que quer simplicidade e a API Python.
- **Apache Iceberg** — várias ferramentas lendo as mesmas tabelas, tabelas muito grandes com
  partições que mudam com o tempo, ou necessidade de um catálogo independente de fornecedor.

!!! note "Convergência"
    Os formatos estão se aproximando: o **Delta UniForm** permite que tabelas Delta sejam lidas
    como Iceberg, e projetos como o **Apache XTable** convertem metadados entre formatos.
