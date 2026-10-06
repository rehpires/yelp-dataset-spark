-- View nao materializada recalculada a cada consulta.
-- Roda na task apply_grants, depois do pipeline, pois depende das tabelas dele.

-- Visao de negocio;
CREATE OR REPLACE VIEW yelp_dataset.gold.v_business_overview
COMMENT 'Negocio + metricas de review, sem dados sensiveis.'
AS
SELECT
  b.business_id,
  b.name,
  b.city,
  b.state,
  b.categories,
  b.is_open,
  b.rating_band,
  coalesce(m.review_count, 0) AS review_count,
  m.avg_stars
FROM yelp_dataset.gold.dim_business b
LEFT JOIN yelp_dataset.gold.fact_business_metrics m
  ON b.business_id = m.business_id;
