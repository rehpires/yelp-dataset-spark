# Yelp Dataset - Data Lakehouse com Databricks e Delta Lake

Pipeline de dados sobre o [Yelp Academic Dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset) (Kaggle), construído como um pipeline **DLT (Lakeflow Declarative Pipelines) em SQL** no Databricks com Unity Catalog. Segue a arquitetura medalhão landing → bronze → silver → gold, orquestrada por Databricks Jobs definidos num Asset Bundle.

## Arquitetura

```mermaid
flowchart LR
    kaggle[("Kaggle\nyelp-dataset")] -->|kagglehub| landing["Volumes\nlanding.*"]
    sim["streaming.ipynb\n(sampler)"] -->|JSON incremental| landing
    landing -->|Auto Loader\nread_files| bronze["Streaming tables\nbronze.*"]
    bronze -->|expectations\n+ AUTO CDC| silver["Streaming tables\nsilver.*"]
    bronze -.->|regras violadas| quarantine["silver.*_quarantine"]
    silver -->|materialized views| gold["gold.*"]
    bronze -->|watermark + janela| window["gold.review_activity_by_window"]
    gold --> view["gold.v_business_overview\n(view comum)"]
```

- **Catálogo**: `yelp_dataset`, isolado do catálogo compartilhado da disciplina e de outros projetos no mesmo workspace.
- **Schemas**: `landing` (Volumes com JSON bruto), `bronze`, `silver`, `gold` e `governance` (função de máscara + event log do pipeline).
- **Pipeline DLT** (`src/dlt/*.sql`, `resources/yelp_dlt_pipeline.yml`): serverless, triggered, dependências inferidas do próprio SQL.

## Camadas

| Camada | Tipo | O que tem |
|---|---|---|
| `landing` | Volumes | JSON Lines original do Kaggle + lotes do simulador em `review/incremental/` |
| `bronze` | Streaming tables | Auto Loader (`STREAM read_files`) com schema explícito, `_rescued_data`, `_source_file`, `_ingested_at`; expectations só medem |
| `silver` | Streaming tables | Expectations com `DROP ROW` (views temporárias `*_validated`), quarentena `*_quarantine`, `AUTO CDC` por chave natural; `review` (SCD 1) + `review_history` (SCD 2); `user.cpf` simulado e mascarado |
| `gold` | Materialized views + 1 streaming table | `dim_business`, `dim_user`, `fact_business_metrics`, `fact_user_metrics`, `summary_metrics`, pivots `business_by_state` e `review_by_month`; `review_activity_by_window` (watermark 10 min, janela 5 min) |

### Qualidade (expectations)

Regras derivadas do schema/colunas ingeridos: chave natural não nula, `_rescued_data IS NULL` (valor fora do tipo declarado), colunas essenciais preenchidas e domínio coerente com o tipo (`stars` 1–5, contadores ≥ 0, datas não futuras, lat/long válidas). Na bronze as regras só geram métricas; na silver o registro é descartado e isolado na quarentena com a lista de regras violadas.

### CDC

O simulador (`src/streaming.ipynb`) amostra reviews da silver, embaralha e grava um lote JSON: uma fração mantém o `review_id` original com `stars` diferente e `date` maior (update), o resto vira insert (`uuid()`), e uma pequena fração recebe nota inválida. O `AUTO CDC ... SEQUENCE BY date` consolida a versão mais recente em `silver.review` e guarda o histórico em `silver.review_history`.

## Governança

- `DCL/create-role.sql`: perfis `data_engineering`, `data_analyst`, `business_analyst` (grupos criados via `databricks groups create`) e a column mask `governance.mask_cpf`, vinculada a `silver.user.cpf` e `gold.dim_user.cpf` na definição do DLT.
- `DCL/grants.sql`: engenharia com acesso total; `data_analyst` lê silver e gold com CPF mascarado; `business_analyst` só lê a gold.
- Lineage landing → bronze → silver → gold → view capturado automaticamente pelo Unity Catalog (screenshots em `Relatório de Entrega/`).

## Otimização e manutenção

- Z-Order (`pipelines.autoOptimize.zOrderCols`) em `silver.review`/`silver.tip`; liquid clustering (`CLUSTER BY`) nas demais tabelas.
- Deletion vectors e Change Data Feed habilitados; manutenção automática do DLT com retenções explícitas (OPTIMIZE já registrado no histórico; VACUUM fica a cargo do ciclo automático).
- O compute é serverless e dimensiona sozinho, porque o Free Edition não permite cluster clássico.

## Orquestração

Dois Databricks Jobs, ambos disparando o mesmo pipeline DLT, com `max_concurrent_runs: 1` + fila e sem full refresh para não haver overlap:

| Job | Tasks | Gatilho |
|---|---|---|
| `yelp_pipeline` | `apply_ddl → download_landing → run_pipeline → apply_grants` | manual (carga inicial/setup) |
| `streaming_simulator` | `run_simulator → run_pipeline` | periódico de 1h (pausado por padrão) |

`apply_ddl` (notebook `src/apply_ddl.ipynb`) aplica uma lista de arquivos SQL: antes do pipeline o catálogo/schemas/volumes e a função de máscara; depois dele as views comuns e os GRANTs, que dependem das tabelas criadas pelo DLT.

## Auditoria e métricas

- `src/audit/time_travel.sql`: `DESCRIBE DETAIL`/`HISTORY`, `VERSION AS OF`, `table_changes` e histórico SCD 2.
- `src/monitoring/dq_report.sql`: Data Quality Report a partir do event log (`governance.pipeline_event_log`), volume por flow, duração dos updates, estratégia de refresh das MVs e teste de data skipping.
- `Relatório de Entrega/checklist-requisitos-lakehouse.md`: mapa requisito → implementação → evidência.

## Como rodar

Requer `uv` (Python 3.12) e o Databricks CLI autenticado no workspace.

```bash
uv sync                                    # dependências locais
databricks bundle validate                 # valida o bundle sem alterar o workspace
databricks bundle deploy                   # cria/atualiza pipeline e jobs no Databricks
databricks bundle run yelp_pipeline        # setup + carga inicial via DLT
databricks bundle run streaming_simulator  # lote incremental (inserts/updates) + update do pipeline
```
