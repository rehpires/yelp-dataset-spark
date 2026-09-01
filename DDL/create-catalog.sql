-- =============================================================================
-- Projeto Yelp Academic Dataset - Unity Catalog: catalogo, camadas landing/bronze
-- =============================================================================

-- Definicao do CATALOGO
CREATE CATALOG IF NOT EXISTS yelp_dataset
COMMENT 'Catalogo do projeto de Big Data com Spark - Yelp Academic Dataset';

-- Camadas (schemas) do pipeline: landing -> bronze -> silver -> gold
CREATE SCHEMA IF NOT EXISTS yelp_dataset.landing
COMMENT 'Arquivos brutos do Yelp Academic Dataset (Kaggle), sem transformacao com foco em auditoria e rastreabilidade';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.bronze
COMMENT 'Ingestao dos arquivos da landing como tabelas Delta, com metadados de ingestao';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.silver
COMMENT 'Dados limpos, deduplicados e conformados';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.gold
COMMENT 'Metricas e agregacoes de negocio';

-- -----------------------------------------------------------------------------
-- Camada LANDING: um volume gerenciado por dataset de origem, para receber os
-- arquivos JSON baixados do Kaggle (yelp_academic_dataset_<nome>.json) antes
-- da leitura pelo Spark.
-- A partir dessa camada, ignoramos qualquer arquivo fora da lista pré-definida.
-- -----------------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.business
COMMENT 'yelp_academic_dataset_business.json (JSON Lines)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.review
COMMENT 'yelp_academic_dataset_review.json (JSON Lines)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.user
COMMENT 'yelp_academic_dataset_user.json (JSON Lines)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.checkin
COMMENT 'yelp_academic_dataset_checkin.json (JSON Lines)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.tip
COMMENT 'yelp_academic_dataset_tip.json (JSON Lines)';

-- Checkpoints do Structured Streaming (src/streaming_consumer.ipynb).
CREATE VOLUME IF NOT EXISTS yelp_dataset.gold.checkpoints
COMMENT 'Checkpoint location das queries de Structured Streaming';

-- -----------------------------------------------------------------------------
-- Camada BRONZE: tabelas Delta com o schema real de cada dataset do Yelp.
-- Campos aninhados/heterogeneos entre registros (attributes, hours, categories
-- em alguns arquivos) sao mantidos como STRING (JSON bruto) para nao quebrar
-- com drift de schema; o parse tipado é aplicado a partir da camada  para a camada Silver.
-- Colunas _source_file/_ingested_at rastreiam a origem de cada linha (lineage).
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yelp_dataset.bronze.business (
  business_id   STRING,
  name          STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  postal_code   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  stars         DOUBLE,
  review_count  BIGINT,
  is_open       BIGINT,
  attributes    STRING,
  categories    STRING,
  hours         STRING,
  _source_file  STRING,
  _ingested_at  TIMESTAMP
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.bronze.review (
  review_id     STRING,
  user_id       STRING,
  business_id   STRING,
  stars         DOUBLE,
  useful        BIGINT,
  funny         BIGINT,
  cool          BIGINT,
  text          STRING,
  date          TIMESTAMP,
  _source_file  STRING,
  _ingested_at  TIMESTAMP
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.bronze.user (
  user_id             STRING,
  name                STRING,
  review_count        BIGINT,
  yelping_since       TIMESTAMP,
  useful              BIGINT,
  funny               BIGINT,
  cool                BIGINT,
  elite               STRING,
  friends             STRING,
  fans                BIGINT,
  average_stars       DOUBLE,
  compliment_hot      BIGINT,
  compliment_more     BIGINT,
  compliment_profile  BIGINT,
  compliment_cute     BIGINT,
  compliment_list     BIGINT,
  compliment_note     BIGINT,
  compliment_plain    BIGINT,
  compliment_cool     BIGINT,
  compliment_funny    BIGINT,
  compliment_writer   BIGINT,
  compliment_photos   BIGINT,
  _source_file        STRING,
  _ingested_at        TIMESTAMP
)
USING DELTA
CLUSTER BY (user_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.bronze.checkin (
  business_id   STRING,
  date          STRING,
  _source_file  STRING,
  _ingested_at  TIMESTAMP
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.bronze.tip (
  user_id           STRING,
  business_id       STRING,
  text              STRING,
  date              TIMESTAMP,
  compliment_count  BIGINT,
  _source_file      STRING,
  _ingested_at      TIMESTAMP
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- -----------------------------------------------------------------------------
-- Camada SILVER: mesmo schema da bronze, sem as colunas de controle de
-- ingestao (_source_file, _ingested_at) e sem duplicatas.
-- fiz o dedup (via ROW_NUMBER particionado pela chave natural de cada dataset) no
-- notebook src/load_silver.ipynb, na carga da bronze para a silver.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yelp_dataset.silver.business (
  business_id   STRING,
  name          STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  postal_code   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  stars         DOUBLE,
  review_count  BIGINT,
  is_open       BIGINT,
  attributes    STRING,
  categories    STRING,
  hours         STRING
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.silver.review (
  review_id     STRING,
  user_id       STRING,
  business_id   STRING,
  stars         DOUBLE,
  useful        BIGINT,
  funny         BIGINT,
  cool          BIGINT,
  text          STRING,
  date          TIMESTAMP
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.silver.user (
  user_id             STRING,
  name                STRING,
  review_count        BIGINT,
  yelping_since       TIMESTAMP,
  useful              BIGINT,
  funny               BIGINT,
  cool                BIGINT,
  elite               STRING,
  friends             STRING,
  fans                BIGINT,
  average_stars       DOUBLE,
  compliment_hot      BIGINT,
  compliment_more     BIGINT,
  compliment_profile  BIGINT,
  compliment_cute     BIGINT,
  compliment_list     BIGINT,
  compliment_note     BIGINT,
  compliment_plain    BIGINT,
  compliment_cool     BIGINT,
  compliment_funny    BIGINT,
  compliment_writer   BIGINT,
  compliment_photos   BIGINT
)
USING DELTA
CLUSTER BY (user_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.silver.checkin (
  business_id   STRING,
  date          STRING
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.silver.tip (
  user_id           STRING,
  business_id       STRING,
  text              STRING,
  date              TIMESTAMP,
  compliment_count  BIGINT
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- -----------------------------------------------------------------------------
-- Camada GOLD: modelagem dimensional simples para BI/Dashboard (dimensoes + metricas pre-agregadas)
-- Populada pelo notebook src/load_gold.ipynb a partir da silver.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yelp_dataset.gold.dim_business (
  business_id  STRING,
  name         STRING,
  city         STRING,
  state        STRING,
  categories   STRING,
  is_open      BIGINT
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

CREATE TABLE IF NOT EXISTS yelp_dataset.gold.dim_user (
  user_id        STRING,
  name           STRING,
  yelping_since  TIMESTAMP
)
USING DELTA
CLUSTER BY (user_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Metrica: contagem de reviews e media de notas (stars) por business.
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.fact_business_metrics (
  business_id   STRING,
  review_count  BIGINT,
  avg_stars     DOUBLE
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Metrica: contagem de reviews e de tips por usuario.
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.fact_user_metrics (
  user_id       STRING,
  review_count  BIGINT,
  tip_count     BIGINT
)
USING DELTA
CLUSTER BY (user_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- KPIs globais de uma linha so: total de reviews, usuarios, businesses e a
-- media de tips por usuario (total de tips / total de usuarios).
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.summary_metrics (
  total_reviews      BIGINT,
  total_users        BIGINT,
  total_businesses   BIGINT,
  avg_tips_per_user  DOUBLE,
  computed_at        TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Pivot: quantidade de business abertos/fechados por estado.
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.business_by_state (
  state          STRING,
  closed_count   BIGINT,
  open_count     BIGINT
)
USING DELTA
CLUSTER BY (state)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Pivot: quantidade de reviews por mes, uma linha por ano.
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.review_by_month (
  year  INT,
  jan   BIGINT,
  fev   BIGINT,
  mar   BIGINT,
  abr   BIGINT,
  mai   BIGINT,
  jun   BIGINT,
  jul   BIGINT,
  ago   BIGINT,
  set   BIGINT,
  out   BIGINT,
  nov   BIGINT,
  dez   BIGINT
)
USING DELTA
CLUSTER BY (year)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Sink do Structured Streaming: contagem de reviews e media de notas por
-- business, agregada em janelas de 5 minutos sobre bronze.review (populada
-- pelo simulador em src/streaming.ipynb). Ver src/streaming_consumer.ipynb.
CREATE TABLE IF NOT EXISTS yelp_dataset.gold.review_activity_by_window (
  business_id    STRING,
  window_start   TIMESTAMP,
  window_end     TIMESTAMP,
  review_count   BIGINT,
  avg_stars      DOUBLE
)
USING DELTA
CLUSTER BY (business_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE yelp_dataset.silver.business ADD COLUMNS IF NOT EXISTS (rating_band STRING);
ALTER TABLE yelp_dataset.gold.dim_business ADD COLUMNS IF NOT EXISTS (rating_band STRING);
