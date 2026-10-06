
-- GOLD
DROP TABLE IF EXISTS yelp_dataset.gold.business_by_state;
DROP TABLE IF EXISTS yelp_dataset.gold.dim_business;
DROP TABLE IF EXISTS yelp_dataset.gold.dim_user;
DROP TABLE IF EXISTS yelp_dataset.gold.fact_business_metrics;
DROP TABLE IF EXISTS yelp_dataset.gold.fact_user_metrics;
DROP TABLE IF EXISTS yelp_dataset.gold.review_activity_by_window;
DROP TABLE IF EXISTS yelp_dataset.gold.review_by_month;
DROP TABLE IF EXISTS yelp_dataset.gold.summary_metrics;

-- SILVER
DROP TABLE IF EXISTS yelp_dataset.silver.business;
DROP TABLE IF EXISTS yelp_dataset.silver.checkin;
DROP TABLE IF EXISTS yelp_dataset.silver.review;
DROP TABLE IF EXISTS yelp_dataset.silver.tip;
DROP TABLE IF EXISTS yelp_dataset.silver.user;

-- BRONZE
DROP TABLE IF EXISTS yelp_dataset.bronze.business;
DROP TABLE IF EXISTS yelp_dataset.bronze.checkin;
DROP TABLE IF EXISTS yelp_dataset.bronze.review;
DROP TABLE IF EXISTS yelp_dataset.bronze.tip;
DROP TABLE IF EXISTS yelp_dataset.bronze.user;

DROP VOLUME IF EXISTS yelp_dataset.gold.checkpoints;
