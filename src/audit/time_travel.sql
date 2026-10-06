-- =============================================================================
-- Auditoria: protocolo de transacao do Delta e time travel
-- =============================================================================
-- Rodar no SQL editor (warehouse serverless) depois de pelo menos dois updates
-- do pipeline: a carga inicial (yelp_pipeline) e um lote do simulador
-- (streaming_simulator). Evidencias usadas no relatorio de arquitetura.
-- =============================================================================

-- 1. Anatomia da tabela: formato, localizacao, numero de arquivos, versoes do
--    protocolo (minReaderVersion/minWriterVersion) e table features ativas
--    (deletionVectors, changeDataFeed, liquid clustering...).
DESCRIBE DETAIL yelp_dataset.silver.review;
SHOW TBLPROPERTIES yelp_dataset.silver.review;

-- 2. Log de transacoes (_delta_log): cada commit vira uma versao, com operacao,
--    parametros e metricas. Mostra os MERGE do AUTO CDC, os OPTIMIZE (com
--    zOrderBy) e VACUUM da manutencao automatica e os deletion vectors gerados.
SELECT
  version,
  timestamp,
  operation,
  operationParameters,
  operationMetrics.numTargetRowsUpdated   AS rows_updated,
  operationMetrics.numTargetRowsInserted  AS rows_inserted,
  operationMetrics.numTargetDeletionVectorsAdded AS deletion_vectors_added,
  operationMetrics.numTargetRowsCopied    AS rows_rewritten,
  operationMetrics.numTargetFilesRemoved  AS removed_files,
  operationMetrics.numTargetFilesAdded    AS added_files
FROM (DESCRIBE HISTORY yelp_dataset.silver.review)
ORDER BY version DESC;

-- Manutencao (compactacao/Z-Order e limpeza) executada pelo DLT.
SELECT version, timestamp, operation, operationParameters, operationMetrics
FROM (DESCRIBE HISTORY yelp_dataset.silver.review)
WHERE operation IN ('OPTIMIZE', 'VACUUM START', 'VACUUM END')
ORDER BY version DESC;

-- 3. Reviews alteradas pelo simulador: historico SCD 2 (uma linha por versao
--    da nota, __END_AT nulo = versao vigente).
SELECT review_id, stars, date, __START_AT, __END_AT
FROM yelp_dataset.silver.review_history
WHERE review_id IN (
  SELECT review_id FROM yelp_dataset.silver.review_history
  GROUP BY review_id HAVING count(*) > 1
)
ORDER BY review_id, __START_AT
LIMIT 50;

-- 4. Time travel: mesma review antes e depois do lote CDC. Trocar <v_antes>
--    pela versao anterior ao ultimo MERGE (consulta 2) e <review_id> por um id
--    da consulta 3.
SELECT 'antes' AS estado, review_id, stars, date
FROM yelp_dataset.silver.review VERSION AS OF <v_antes>
WHERE review_id = '<review_id>'
UNION ALL
SELECT 'depois', review_id, stars, date
FROM yelp_dataset.silver.review
WHERE review_id = '<review_id>';

-- Contagem por versao/data: permite auditar o estado da tabela em qualquer
-- ponto da retencao (delta.logRetentionDuration).
SELECT count(*) FROM yelp_dataset.silver.review TIMESTAMP AS OF date_sub(current_date(), 1);

-- 5. Change Data Feed: o que mudou entre versoes (insert, update_preimage,
--    update_postimage), base para propagar mudancas a consumidores externos.
SELECT _change_type, _commit_version, review_id, stars, date
FROM table_changes('yelp_dataset.silver.review', <v_antes>)
WHERE _change_type IN ('update_preimage', 'update_postimage')
ORDER BY review_id, _commit_version, _change_type
LIMIT 50;
