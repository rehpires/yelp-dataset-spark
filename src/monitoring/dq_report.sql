WITH events AS (
  SELECT
    origin.update_id,
    origin.flow_name,
    timestamp,
    explode(from_json(
      details:flow_progress:data_quality:expectations,
      'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
    )) AS e
  FROM yelp_dataset.governance.pipeline_event_log
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
),
-- review_validated alimenta dois flows (review_cdc e review_history_cdc) e cada
-- um reporta as mesmas expectations: soma por flow e fica com o maior valor por
-- update para nao contar cada registro duas vezes.
expectations AS (
  SELECT update_id, min(update_time) AS timestamp, named_struct(
    'dataset', dataset, 'name', name,
    'passed_records', max(passed_records), 'failed_records', max(failed_records)
  ) AS e
  FROM (
    SELECT update_id, flow_name, e.dataset, e.name, min(timestamp) AS update_time,
      sum(e.passed_records) AS passed_records, sum(e.failed_records) AS failed_records
    FROM events
    GROUP BY update_id, flow_name, e.dataset, e.name
  )
  GROUP BY update_id, dataset, name
)
SELECT
  e.dataset,
  e.name AS expectation,
  sum(e.passed_records) AS passed_records,
  sum(e.failed_records) AS failed_records,
  round(100 * sum(e.failed_records) / nullif(sum(e.passed_records) + sum(e.failed_records), 0), 4) AS failed_pct
FROM expectations
GROUP BY e.dataset, e.name
ORDER BY e.dataset, e.name;

-- Relatorio por update (evolucao: carga inicial x lotes incrementais).
WITH events AS (
  SELECT
    origin.update_id,
    origin.flow_name,
    timestamp,
    explode(from_json(
      details:flow_progress:data_quality:expectations,
      'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
    )) AS e
  FROM yelp_dataset.governance.pipeline_event_log
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
),
-- review_validated alimenta dois flows (review_cdc e review_history_cdc) e cada
-- um reporta as mesmas expectations: soma por flow e fica com o maior valor por
-- update para nao contar cada registro duas vezes.
expectations AS (
  SELECT update_id, min(update_time) AS timestamp, named_struct(
    'dataset', dataset, 'name', name,
    'passed_records', max(passed_records), 'failed_records', max(failed_records)
  ) AS e
  FROM (
    SELECT update_id, flow_name, e.dataset, e.name, min(timestamp) AS update_time,
      sum(e.passed_records) AS passed_records, sum(e.failed_records) AS failed_records
    FROM events
    GROUP BY update_id, flow_name, e.dataset, e.name
  )
  GROUP BY update_id, dataset, name
)
SELECT
  update_id,
  min(timestamp) AS update_time,
  e.dataset,
  e.name AS expectation,
  sum(e.passed_records) AS passed_records,
  sum(e.failed_records) AS failed_records
FROM expectations
GROUP BY update_id, e.dataset, e.name
ORDER BY update_time DESC, e.dataset, e.name;

-- Registros isolados na quarentena, por regra violada.
SELECT 'review' AS dataset, _failed_rules, count(*) AS records FROM yelp_dataset.silver.review_quarantine GROUP BY _failed_rules
UNION ALL
SELECT 'business', _failed_rules, count(*) FROM yelp_dataset.silver.business_quarantine GROUP BY _failed_rules
UNION ALL
SELECT 'user', _failed_rules, count(*) FROM yelp_dataset.silver.user_quarantine GROUP BY _failed_rules
UNION ALL
SELECT 'checkin', _failed_rules, count(*) FROM yelp_dataset.silver.checkin_quarantine GROUP BY _failed_rules
UNION ALL
SELECT 'tip', _failed_rules, count(*) FROM yelp_dataset.silver.tip_quarantine GROUP BY _failed_rules
ORDER BY dataset, records DESC;

-- Volume processado por flow e update apos a carga inicial,
-- o Auto Loader processa so o lote novo (num_output_rows ~ batch_size).
-- Flows de streaming reportam as metricas nos eventos com status RUNNING;
-- flows AUTO CDC reportam num_upserted_rows/num_deleted_rows.
SELECT
  origin.update_id,
  origin.flow_name,
  min(timestamp) AS started_at,
  sum(cast(details:flow_progress:metrics:num_output_rows AS BIGINT)) AS output_rows,
  sum(cast(details:flow_progress:metrics:num_upserted_rows AS BIGINT)) AS upserted_rows,
  sum(cast(details:flow_progress:metrics:num_deleted_rows AS BIGINT)) AS deleted_rows,
  sum(cast(details:flow_progress:data_quality:dropped_records AS BIGINT)) AS dropped_records,
  max(cast(details:flow_progress:metrics:backlog_bytes AS BIGINT)) AS backlog_bytes
FROM yelp_dataset.governance.pipeline_event_log
WHERE event_type = 'flow_progress'
GROUP BY origin.update_id, origin.flow_name
HAVING output_rows IS NOT NULL OR upserted_rows IS NOT NULL
ORDER BY started_at DESC, origin.flow_name;

-- Duracao de cada update do pipeline (inicio -> estado final).
SELECT
  origin.update_id,
  min(timestamp) AS started_at,
  max(timestamp) AS finished_at,
  timestampdiff(SECOND, min(timestamp), max(timestamp)) AS duration_s,
  max_by(details:update_progress:state, timestamp) AS final_state
FROM yelp_dataset.governance.pipeline_event_log
WHERE event_type = 'update_progress'
GROUP BY origin.update_id
ORDER BY started_at DESC;

--  Refresh das materialized views (planning_information):
--  incremental (ex.: ROW_BASED, GROUP_AGGREGATE) x COMPLETE_RECOMPUTE. Mostra
--  o ganho de processar so as mudancas da silver em vez de recalcular tudo.
-- A tecnica aplicada e a entrada com is_chosen = true (a ordem do array varia).
SELECT
  timestamp,
  origin.update_id,
  origin.flow_name,
  details:planning_information:incrementalization_status::STRING AS status,
  filter(
    from_json(details:planning_information:technique_information, 'array<struct<maintenance_type: string, is_chosen: boolean>>'),
    t -> t.is_chosen
  )[0].maintenance_type AS technique,
  details:planning_information:source_table_information[0]:num_rows::BIGINT AS source_rows,
  details:planning_information:source_table_information[0]:num_changed_rows::BIGINT AS changed_rows
FROM yelp_dataset.governance.pipeline_event_log
WHERE event_type = 'planning_information'
ORDER BY timestamp DESC, origin.flow_name;

--  Impacto do Z-Order (data skipping): rodar a consulta abaixo e comparar no
--  Query Profile "files read" x "files pruned" e o tempo, antes e depois do
--  OPTIMIZE com zOrderBy registrado em DESCRIBE HISTORY (src/audit/time_travel.sql).
SELECT business_id, count(*) AS reviews, avg(stars) AS avg_stars
FROM yelp_dataset.silver.review
WHERE business_id = 'XQfwVwDr-v0ZS3_CbbE5Xw'
  AND date >= '2018-01-01'
GROUP BY business_id;
