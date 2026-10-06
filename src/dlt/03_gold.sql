-- =============================================================================
-- DLT - Camada GOLD
-- =============================================================================
-- Modelagem dimensional para BI. Dois tipos de dataset:
--   - MATERIALIZED VIEW: resultado pre-computado de uma consulta batch sobre a
--     silver; o DLT recalcula (incrementalmente quando possivel) a cada update.
--     Adequado para agregacoes/joins sobre tabelas que recebem updates (CDC).
--   - STREAMING TABLE review_activity_by_window: agregacao em janela com
--     watermark sobre a bronze append-only (porta do streaming_consumer).
-- A view comum (nao materializada) fica fora do pipeline: DDL/create-views.sql.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.dim_business
COMMENT 'Dimensao de negocios (atributos descritivos + rating_band)'
CLUSTER BY (business_id)
AS SELECT business_id, name, city, state, categories, is_open, rating_band
FROM yelp_dataset.silver.business;

-- cpf propagado com a mesma column mask da silver.
CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.dim_user (
  user_id        STRING,
  name           STRING,
  yelping_since  TIMESTAMP,
  cpf            STRING MASK yelp_dataset.governance.mask_cpf COMMENT 'Dado sensivel simulado (mascarado para analistas)'
)
COMMENT 'Dimensao de usuarios, com CPF mascarado para analistas'
CLUSTER BY (user_id)
AS SELECT user_id, name, yelping_since, cpf
FROM yelp_dataset.silver.user;

CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.fact_business_metrics
COMMENT 'Contagem de reviews e media de estrelas por negocio (reflete updates de nota via CDC)'
CLUSTER BY (business_id)
AS SELECT
  business_id,
  count(*)   AS review_count,
  avg(stars) AS avg_stars
FROM yelp_dataset.silver.review
GROUP BY business_id;

CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.fact_user_metrics
COMMENT 'Contagem de reviews e de tips por usuario'
CLUSTER BY (user_id)
AS
WITH review_counts AS (
  SELECT user_id, count(*) AS review_count FROM yelp_dataset.silver.review GROUP BY user_id
),
tip_counts AS (
  SELECT user_id, count(*) AS tip_count FROM yelp_dataset.silver.tip GROUP BY user_id
)
SELECT
  coalesce(r.user_id, t.user_id) AS user_id,
  coalesce(r.review_count, 0)    AS review_count,
  coalesce(t.tip_count, 0)       AS tip_count
FROM review_counts r
FULL OUTER JOIN tip_counts t ON r.user_id = t.user_id;

CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.summary_metrics
COMMENT 'KPIs globais: totais de reviews, usuarios, negocios e media de tips por usuario'
AS
WITH totals AS (
  SELECT
    (SELECT count(*) FROM yelp_dataset.silver.review)   AS total_reviews,
    (SELECT count(*) FROM yelp_dataset.silver.user)     AS total_users,
    (SELECT count(*) FROM yelp_dataset.silver.business) AS total_businesses,
    (SELECT count(*) FROM yelp_dataset.silver.tip)      AS total_tips
)
SELECT
  total_reviews,
  total_users,
  total_businesses,
  CASE WHEN total_users > 0 THEN total_tips / total_users ELSE 0D END AS avg_tips_per_user,
  current_timestamp() AS computed_at
FROM totals;

-- Pivot: negocios abertos/fechados por estado.
CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.business_by_state
COMMENT 'Quantidade de negocios abertos/fechados por estado (pivot)'
CLUSTER BY (state)
AS SELECT
  state,
  coalesce(closed_count, 0) AS closed_count,
  coalesce(open_count, 0)   AS open_count
FROM (SELECT state, is_open FROM yelp_dataset.silver.business)
PIVOT (count(*) FOR is_open IN (0 AS closed_count, 1 AS open_count));

-- Pivot: reviews por mes, uma linha por ano.
CREATE OR REFRESH MATERIALIZED VIEW yelp_dataset.gold.review_by_month
COMMENT 'Quantidade de reviews por mes, uma linha por ano (pivot)'
CLUSTER BY (year)
AS SELECT
  year,
  coalesce(jan, 0) AS jan, coalesce(fev, 0) AS fev, coalesce(mar, 0) AS mar,
  coalesce(abr, 0) AS abr, coalesce(mai, 0) AS mai, coalesce(jun, 0) AS jun,
  coalesce(jul, 0) AS jul, coalesce(ago, 0) AS ago, coalesce(set, 0) AS set,
  coalesce(out, 0) AS out, coalesce(nov, 0) AS nov, coalesce(dez, 0) AS dez
FROM (SELECT year(date) AS year, month(date) AS month FROM yelp_dataset.silver.review)
PIVOT (count(*) FOR month IN (
  1 AS jan, 2 AS fev, 3 AS mar, 4 AS abr, 5 AS mai, 6 AS jun,
  7 AS jul, 8 AS ago, 9 AS set, 10 AS out, 11 AS nov, 12 AS dez
));

-- Agregacao em janelas de 5 minutos com watermark de 10 minutos: le a bronze
-- (append-only; a silver.review recebe updates do CDC e nao serve de fonte de
-- streaming). Janelas so sao emitidas depois que o watermark passa do fim
-- delas; eventos com date mais antiga que o watermark sao descartados -- caso
-- dos updates de nota do simulador, que reaproveitam datas historicas. O
-- estado da agregacao e o offset do stream ficam no checkpoint gerenciado
-- pelo DLT, garantindo continuidade entre updates.
CREATE OR REFRESH STREAMING TABLE yelp_dataset.gold.review_activity_by_window
COMMENT 'Reviews e media de estrelas por negocio em janelas de 5 minutos (watermark 10 min)'
CLUSTER BY (business_id)
AS SELECT
  business_id,
  window.start AS window_start,
  window.end   AS window_end,
  count(*)     AS review_count,
  avg(stars)   AS avg_stars
FROM STREAM(yelp_dataset.bronze.review)
  WATERMARK date DELAY OF INTERVAL 10 MINUTES
WHERE review_id IS NOT NULL AND stars BETWEEN 1 AND 5
GROUP BY window(date, '5 minutes'), business_id;
