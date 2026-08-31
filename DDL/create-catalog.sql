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
-- Camadas SILVER e GOLD: schemas ja criados acima; tabelas a definir quando as
-- regras de negocio e as transformacoes forem implementadas.
-- -----------------------------------------------------------------------------
