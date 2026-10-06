-- =============================================================================
-- DLT - Camada SILVER
-- =============================================================================
-- Para cada dataset:
--   1. <ds>_validated (TEMPORARY VIEW, interna ao pipeline): le a bronze em
--      streaming e aplica as expectations com ON VIOLATION DROP ROW. As regras
--      derivam do schema/colunas ingeridos (src/config.py):
--        - pk_not_null : chave natural (DEDUP_KEYS) obrigatoria
--        - type_ok     : _rescued_data IS NULL (nenhum valor fora do tipo)
--        - not_null    : colunas TIMESTAMP/DOUBLE essenciais preenchidas
--        - dominio     : faixas coerentes com o tipo (stars 1-5, contadores >= 0,
--                        datas nao futuras, lat/long validas)
--   2. silver.<ds>_quarantine (STREAMING TABLE): as mesmas regras invertidas,
--      isolando os registros rejeitados com a lista de regras violadas.
--   3. silver.<ds> (STREAMING TABLE, alvo de AUTO CDC): consolida inserts e
--      updates pela chave natural, substituindo o ROW_NUMBER dos notebooks.
--
-- CDC de reviews: o simulador reenvia review_id ja existentes com stars novo e
-- date maior; SEQUENCE BY date garante que so a versao mais recente fica em
-- silver.review (SCD 1) e todo o historico fica em silver.review_history (SCD 2).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- REVIEW (CDC)
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW review_validated (
  CONSTRAINT pk_not_null    EXPECT (review_id IS NOT NULL AND business_id IS NOT NULL AND user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT type_ok        EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT stars_in_range EXPECT (stars IS NOT NULL AND stars BETWEEN 1 AND 5) ON VIOLATION DROP ROW,
  CONSTRAINT date_valid     EXPECT (date IS NOT NULL AND date <= current_timestamp() + INTERVAL 1 DAY) ON VIOLATION DROP ROW,
  CONSTRAINT counters_ok    EXPECT (coalesce(useful, 0) >= 0 AND coalesce(funny, 0) >= 0 AND coalesce(cool, 0) >= 0) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(yelp_dataset.bronze.review);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.review_quarantine
COMMENT 'Reviews rejeitadas pelas expectations da silver, com as regras violadas'
AS SELECT
  *,
  concat_ws(',',
    CASE WHEN NOT (review_id IS NOT NULL AND business_id IS NOT NULL AND user_id IS NOT NULL) THEN 'pk_not_null' END,
    CASE WHEN _rescued_data IS NOT NULL THEN 'type_ok' END,
    CASE WHEN NOT coalesce(stars BETWEEN 1 AND 5, false) THEN 'stars_in_range' END,
    CASE WHEN NOT coalesce(date <= current_timestamp() + INTERVAL 1 DAY, false) THEN 'date_valid' END,
    CASE WHEN NOT (coalesce(useful, 0) >= 0 AND coalesce(funny, 0) >= 0 AND coalesce(cool, 0) >= 0) THEN 'counters_ok' END
  ) AS _failed_rules
FROM STREAM(yelp_dataset.bronze.review)
WHERE NOT (
  review_id IS NOT NULL AND business_id IS NOT NULL AND user_id IS NOT NULL
  AND _rescued_data IS NULL
  AND coalesce(stars BETWEEN 1 AND 5, false)
  AND coalesce(date <= current_timestamp() + INTERVAL 1 DAY, false)
  AND coalesce(useful, 0) >= 0 AND coalesce(funny, 0) >= 0 AND coalesce(cool, 0) >= 0
);

-- Estado atual de cada review (SCD 1). Z-Order em business_id/date (consultas
-- da gold filtram/agrupam por esses campos); liquid clustering nao e usado aqui
-- porque e mutuamente exclusivo com Z-Order. A manutencao automatica do DLT
-- (autoOptimize.managed) roda OPTIMIZE + VACUUM; as retencoes definem por quanto
-- tempo o time travel alcanca versoes antigas (log) e arquivos removidos.
CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.review
COMMENT 'Review validada, versao mais recente por review_id (AUTO CDC SCD 1)'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'pipelines.autoOptimize.zOrderCols' = 'business_id,date',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

CREATE FLOW review_cdc AS AUTO CDC INTO yelp_dataset.silver.review
FROM STREAM(review_validated)
KEYS (review_id)
SEQUENCE BY date
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 1;

-- Historico completo das alteracoes de nota (SCD 2: __START_AT / __END_AT),
-- usado para auditoria junto com o time travel do Delta.
CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.review_history
COMMENT 'Historico de versoes de cada review (AUTO CDC SCD 2)'
CLUSTER BY (review_id)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE FLOW review_history_cdc AS AUTO CDC INTO yelp_dataset.silver.review_history
FROM STREAM(review_validated)
KEYS (review_id)
SEQUENCE BY date
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 2
TRACK HISTORY ON stars, date;


-- -----------------------------------------------------------------------------
-- BUSINESS
-- -----------------------------------------------------------------------------
-- rating_band (Baixa/Media/Alta) substitui a UDF Python classify_rating.
CREATE TEMPORARY VIEW business_validated (
  CONSTRAINT pk_not_null    EXPECT (business_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT type_ok        EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT stars_in_range EXPECT (stars IS NOT NULL AND stars BETWEEN 1 AND 5) ON VIOLATION DROP ROW,
  CONSTRAINT coords_valid   EXPECT (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180) ON VIOLATION DROP ROW,
  CONSTRAINT counters_ok    EXPECT (coalesce(review_count, 0) >= 0 AND is_open IN (0, 1)) ON VIOLATION DROP ROW
)
AS SELECT
  *,
  CASE
    WHEN stars < 3 THEN 'Baixa'
    WHEN stars < 4 THEN 'Media'
    ELSE 'Alta'
  END AS rating_band
FROM STREAM(yelp_dataset.bronze.business);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.business_quarantine
COMMENT 'Businesses rejeitados pelas expectations da silver, com as regras violadas'
AS SELECT
  *,
  concat_ws(',',
    CASE WHEN business_id IS NULL THEN 'pk_not_null' END,
    CASE WHEN _rescued_data IS NOT NULL THEN 'type_ok' END,
    CASE WHEN NOT coalesce(stars BETWEEN 1 AND 5, false) THEN 'stars_in_range' END,
    CASE WHEN NOT coalesce(latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180, false) THEN 'coords_valid' END,
    CASE WHEN NOT coalesce(coalesce(review_count, 0) >= 0 AND is_open IN (0, 1), false) THEN 'counters_ok' END
  ) AS _failed_rules
FROM STREAM(yelp_dataset.bronze.business)
WHERE NOT (
  business_id IS NOT NULL
  AND _rescued_data IS NULL
  AND coalesce(stars BETWEEN 1 AND 5, false)
  AND coalesce(latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180, false)
  AND coalesce(coalesce(review_count, 0) >= 0 AND is_open IN (0, 1), false)
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.business
COMMENT 'Business validado e deduplicado por business_id (AUTO CDC SCD 1)'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true');

CREATE FLOW business_cdc AS AUTO CDC INTO yelp_dataset.silver.business
FROM STREAM(business_validated)
KEYS (business_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 1;


-- -----------------------------------------------------------------------------
-- USER (com CPF simulado e mascarado)
-- -----------------------------------------------------------------------------
-- O dataset do Yelp nao tem dado pessoal sensivel; cpf e simulado de forma
-- deterministica a partir do user_id (mesmo usuario -> mesmo CPF a cada carga),
-- no formato 000.000.000-00. A column mask governance.mask_cpf
-- (DCL/create-role.sql) esconde o valor para data_analyst e business_analyst.
CREATE TEMPORARY VIEW user_validated (
  CONSTRAINT pk_not_null  EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT type_ok      EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT since_valid  EXPECT (yelping_since IS NOT NULL AND yelping_since <= current_timestamp()) ON VIOLATION DROP ROW,
  CONSTRAINT stars_valid  EXPECT (average_stars IS NULL OR average_stars BETWEEN 0 AND 5) ON VIOLATION DROP ROW,
  CONSTRAINT counters_ok  EXPECT (coalesce(review_count, 0) >= 0 AND coalesce(fans, 0) >= 0) ON VIOLATION DROP ROW
)
AS SELECT
  * EXCEPT (cpf_digits),
  concat(
    substr(cpf_digits, 1, 3), '.', substr(cpf_digits, 4, 3), '.',
    substr(cpf_digits, 7, 3), '-', substr(cpf_digits, 10, 2)
  ) AS cpf
FROM (
  SELECT
    *,
    lpad(cast(pmod(xxhash64(user_id), 100000000000) AS STRING), 11, '0') AS cpf_digits
  FROM STREAM(yelp_dataset.bronze.user)
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.user_quarantine
COMMENT 'Users rejeitados pelas expectations da silver, com as regras violadas'
AS SELECT
  *,
  concat_ws(',',
    CASE WHEN user_id IS NULL THEN 'pk_not_null' END,
    CASE WHEN _rescued_data IS NOT NULL THEN 'type_ok' END,
    CASE WHEN NOT coalesce(yelping_since <= current_timestamp(), false) THEN 'since_valid' END,
    CASE WHEN NOT coalesce(average_stars IS NULL OR average_stars BETWEEN 0 AND 5, false) THEN 'stars_valid' END,
    CASE WHEN NOT (coalesce(review_count, 0) >= 0 AND coalesce(fans, 0) >= 0) THEN 'counters_ok' END
  ) AS _failed_rules
FROM STREAM(yelp_dataset.bronze.user)
WHERE NOT (
  user_id IS NOT NULL
  AND _rescued_data IS NULL
  AND coalesce(yelping_since <= current_timestamp(), false)
  AND coalesce(average_stars IS NULL OR average_stars BETWEEN 0 AND 5, false)
  AND coalesce(review_count, 0) >= 0 AND coalesce(fans, 0) >= 0
);

-- Schema explicito para poder vincular a column mask ao cpf.
CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.user (
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
  cpf                 STRING MASK yelp_dataset.governance.mask_cpf COMMENT 'Dado sensivel simulado (mascarado para analistas)'
)
COMMENT 'User validado e deduplicado por user_id (AUTO CDC SCD 1), com CPF mascarado'
CLUSTER BY (user_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true');

CREATE FLOW user_cdc AS AUTO CDC INTO yelp_dataset.silver.user
FROM STREAM(user_validated)
KEYS (user_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 1;


-- -----------------------------------------------------------------------------
-- CHECKIN
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW checkin_validated (
  CONSTRAINT pk_not_null EXPECT (business_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT date_filled EXPECT (date IS NOT NULL AND date <> '') ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(yelp_dataset.bronze.checkin);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.checkin_quarantine
COMMENT 'Checkins rejeitados pelas expectations da silver, com as regras violadas'
AS SELECT
  *,
  concat_ws(',',
    CASE WHEN business_id IS NULL THEN 'pk_not_null' END,
    CASE WHEN _rescued_data IS NOT NULL THEN 'type_ok' END,
    CASE WHEN NOT coalesce(date <> '', false) THEN 'date_filled' END
  ) AS _failed_rules
FROM STREAM(yelp_dataset.bronze.checkin)
WHERE NOT (business_id IS NOT NULL AND _rescued_data IS NULL AND coalesce(date <> '', false));

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.checkin
COMMENT 'Checkin validado e deduplicado por business_id (AUTO CDC SCD 1)'
CLUSTER BY (business_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true');

CREATE FLOW checkin_cdc AS AUTO CDC INTO yelp_dataset.silver.checkin
FROM STREAM(checkin_validated)
KEYS (business_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 1;


-- -----------------------------------------------------------------------------
-- TIP
-- -----------------------------------------------------------------------------
-- tip nao tem id proprio: chave composta (user_id, business_id, date), como em
-- DEDUP_KEYS. O arquivo original tem 79 chaves repetidas no mesmo lote (mesmo
-- _ingested_at), entao text/compliment_count entram no SEQUENCE BY como
-- desempate deterministico.
CREATE TEMPORARY VIEW tip_validated (
  CONSTRAINT pk_not_null EXPECT (user_id IS NOT NULL AND business_id IS NOT NULL AND date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT type_ok     EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT date_valid  EXPECT (date <= current_timestamp() + INTERVAL 1 DAY) ON VIOLATION DROP ROW,
  CONSTRAINT counters_ok EXPECT (coalesce(compliment_count, 0) >= 0) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(yelp_dataset.bronze.tip);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.tip_quarantine
COMMENT 'Tips rejeitadas pelas expectations da silver, com as regras violadas'
AS SELECT
  *,
  concat_ws(',',
    CASE WHEN NOT (user_id IS NOT NULL AND business_id IS NOT NULL AND date IS NOT NULL) THEN 'pk_not_null' END,
    CASE WHEN _rescued_data IS NOT NULL THEN 'type_ok' END,
    CASE WHEN NOT coalesce(date <= current_timestamp() + INTERVAL 1 DAY, false) THEN 'date_valid' END,
    CASE WHEN NOT (coalesce(compliment_count, 0) >= 0) THEN 'counters_ok' END
  ) AS _failed_rules
FROM STREAM(yelp_dataset.bronze.tip)
WHERE NOT (
  user_id IS NOT NULL AND business_id IS NOT NULL AND date IS NOT NULL
  AND _rescued_data IS NULL
  AND coalesce(date <= current_timestamp() + INTERVAL 1 DAY, false)
  AND coalesce(compliment_count, 0) >= 0
);

CREATE OR REFRESH STREAMING TABLE yelp_dataset.silver.tip
COMMENT 'Tip validada e deduplicada por (user_id, business_id, date) (AUTO CDC SCD 1)'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'pipelines.autoOptimize.zOrderCols' = 'business_id,date',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

CREATE FLOW tip_cdc AS AUTO CDC INTO yelp_dataset.silver.tip
FROM STREAM(tip_validated)
KEYS (user_id, business_id, date)
SEQUENCE BY STRUCT(_ingested_at, text, compliment_count)
COLUMNS * EXCEPT (_source_file, _ingested_at, _rescued_data)
STORED AS SCD TYPE 1;
