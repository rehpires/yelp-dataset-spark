-- data_engineering: dono operacional de todo o catalogo
GRANT ALL PRIVILEGES ON CATALOG yelp_dataset TO `data_engineering`;

-- data_analyst: leitura em silver e gold (CPF mascarado via column mask)
GRANT USE CATALOG ON CATALOG yelp_dataset TO `data_analyst`;
GRANT USE SCHEMA, SELECT ON SCHEMA yelp_dataset.silver TO `data_analyst`;
GRANT USE SCHEMA, SELECT ON SCHEMA yelp_dataset.gold TO `data_analyst`;
GRANT USE SCHEMA ON SCHEMA yelp_dataset.governance TO `data_analyst`;
GRANT EXECUTE ON FUNCTION yelp_dataset.governance.mask_cpf TO `data_analyst`;

-- business_analyst: apenas gold (isolamento das camadas bronze/silver/landing)
GRANT USE CATALOG ON CATALOG yelp_dataset TO `business_analyst`;
GRANT USE SCHEMA, SELECT ON SCHEMA yelp_dataset.gold TO `business_analyst`;
GRANT USE SCHEMA ON SCHEMA yelp_dataset.governance TO `business_analyst`;
GRANT EXECUTE ON FUNCTION yelp_dataset.governance.mask_cpf TO `business_analyst`;
