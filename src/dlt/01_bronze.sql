-- =============================================================================
-- DLT - Camada BRONZE
-- =============================================================================
-- Streaming tables append-only alimentadas pelo Auto Loader (read_files em modo
-- STREAM): cada update do pipeline processa apenas os arquivos novos que
-- chegaram no Volume da landing desde o ultimo checkpoint (gerenciado pelo DLT).
--
-- O schema e declarado explicitamente (espelha BRONZE_SCHEMAS em src/config.py).
-- Campos aninhados/heterogeneos (attributes, hours) ficam como STRING com o JSON
-- bruto. Qualquer valor que nao case com o tipo declarado vira NULL e o original
-- vai para _rescued_data -- base da expectation type_ok.
--
-- Na bronze as expectations sao so de observacao (sem ON VIOLATION): o dado
-- bruto e preservado integralmente e as metricas de qualidade ficam no event
-- log. O descarte/isolamento acontece na silver.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE yelp_dataset.bronze.business (
  CONSTRAINT pk_not_null EXPECT (business_id IS NOT NULL),
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL)
)
COMMENT 'Yelp business bruto, ingerido incrementalmente via Auto Loader'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/yelp_dataset/landing/business',
  format => 'json',
  pathGlobFilter => '*.json',
  rescuedDataColumn => '_rescued_data',
  schema => 'business_id STRING, name STRING, address STRING, city STRING, state STRING, postal_code STRING, latitude DOUBLE, longitude DOUBLE, stars DOUBLE, review_count BIGINT, is_open BIGINT, attributes STRING, categories STRING, hours STRING'
);

-- review tambem recebe os lotes do simulador (src/streaming.ipynb) em
-- review/incremental/: inserts (review_id novo) e updates (review_id existente,
-- stars novo e date maior), consolidados via AUTO CDC na silver.
CREATE OR REFRESH STREAMING TABLE yelp_dataset.bronze.review (
  CONSTRAINT pk_not_null    EXPECT (review_id IS NOT NULL),
  CONSTRAINT type_ok        EXPECT (_rescued_data IS NULL),
  CONSTRAINT stars_in_range EXPECT (stars BETWEEN 1 AND 5)
)
COMMENT 'Yelp review bruto (carga Kaggle + lotes incrementais do simulador), via Auto Loader'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/yelp_dataset/landing/review',
  format => 'json',
  pathGlobFilter => '*.json',
  rescuedDataColumn => '_rescued_data',
  schema => 'review_id STRING, user_id STRING, business_id STRING, stars DOUBLE, useful BIGINT, funny BIGINT, cool BIGINT, text STRING, date TIMESTAMP'
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.bronze.user (
  CONSTRAINT pk_not_null EXPECT (user_id IS NOT NULL),
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL)
)
COMMENT 'Yelp user bruto, ingerido incrementalmente via Auto Loader'
CLUSTER BY (user_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/yelp_dataset/landing/user',
  format => 'json',
  pathGlobFilter => '*.json',
  rescuedDataColumn => '_rescued_data',
  schema => 'user_id STRING, name STRING, review_count BIGINT, yelping_since TIMESTAMP, useful BIGINT, funny BIGINT, cool BIGINT, elite STRING, friends STRING, fans BIGINT, average_stars DOUBLE, compliment_hot BIGINT, compliment_more BIGINT, compliment_profile BIGINT, compliment_cute BIGINT, compliment_list BIGINT, compliment_note BIGINT, compliment_plain BIGINT, compliment_cool BIGINT, compliment_funny BIGINT, compliment_writer BIGINT, compliment_photos BIGINT'
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.bronze.checkin (
  CONSTRAINT pk_not_null EXPECT (business_id IS NOT NULL),
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL)
)
COMMENT 'Yelp checkin bruto, ingerido incrementalmente via Auto Loader'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/yelp_dataset/landing/checkin',
  format => 'json',
  pathGlobFilter => '*.json',
  rescuedDataColumn => '_rescued_data',
  schema => 'business_id STRING, date STRING'
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.bronze.tip (
  CONSTRAINT pk_not_null EXPECT (user_id IS NOT NULL AND business_id IS NOT NULL AND date IS NOT NULL),
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL)
)
COMMENT 'Yelp tip bruto, ingerido incrementalmente via Auto Loader'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/yelp_dataset/landing/tip',
  format => 'json',
  pathGlobFilter => '*.json',
  rescuedDataColumn => '_rescued_data',
  schema => 'user_id STRING, business_id STRING, text STRING, date TIMESTAMP, compliment_count BIGINT'
);
