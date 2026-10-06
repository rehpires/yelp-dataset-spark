-- Definicao do CATALOGO
CREATE CATALOG IF NOT EXISTS yelp_dataset
COMMENT 'Catalogo do projeto de Big Data com Spark - Yelp Academic Dataset';

-- Camadas (schemas) do pipeline: landing -> bronze -> silver -> gold
CREATE SCHEMA IF NOT EXISTS yelp_dataset.landing
COMMENT 'Arquivos brutos do Yelp Academic Dataset (Kaggle), sem transformacao com foco em auditoria e rastreabilidade';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.bronze
COMMENT 'Streaming tables do DLT: ingestao incremental (Auto Loader) dos arquivos da landing, com metadados de ingestao';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.silver
COMMENT 'Dados validados (expectations), deduplicados/CDC (AUTO CDC) e quarentena dos registros rejeitados';

CREATE SCHEMA IF NOT EXISTS yelp_dataset.gold
COMMENT 'Materialized views de negocio e agregacao em janela via streaming';

-- Schema de governanca: funcoes de mascaramento (DCL/create-role.sql) e o
-- event log do pipeline DLT (resources/yelp_dlt_pipeline.yml).
CREATE SCHEMA IF NOT EXISTS yelp_dataset.governance
COMMENT 'Funcoes de mascaramento (column masks) e event log do pipeline DLT';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.business
COMMENT 'yelp_academic_dataset_business.json (Line JSON)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.review
COMMENT 'yelp_academic_dataset_review.json (Line JSON) + lotes incrementais';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.user
COMMENT 'yelp_academic_dataset_user.json (Line JSON)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.checkin
COMMENT 'yelp_academic_dataset_checkin.json (Line JSON)';

CREATE VOLUME IF NOT EXISTS yelp_dataset.landing.tip
COMMENT 'yelp_academic_dataset_tip.json (Line JSON)';
