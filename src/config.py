from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "yelp_dataset"
LANDING_SCHEMA = "landing"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

DATASETS = ["business", "review", "user", "checkin", "tip"]
STAGING_DIR = "/local_disk0/tmp/kagglehub_staging"

# Chave natural de cada dataset (KEYS do AUTO CDC em src/dlt/02_silver.sql).
DEDUP_KEYS = {
    "business": ["business_id"],
    "review": ["review_id"],
    "user": ["user_id"],
    "checkin": ["business_id"],
    "tip": ["user_id", "business_id", "date"],
}


def source_filename(dataset: str) -> str:
    return f"yelp_academic_dataset_{dataset}.json"

def landing_volume_dir(dataset: str) -> str:
    return f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{dataset}"


def landing_file_path(dataset: str) -> str:
    return f"{landing_volume_dir(dataset)}/{source_filename(dataset)}"


# Lotes do simulador (src/streaming.ipynb), capturados pelo Auto Loader do DLT
# junto com o arquivo original, que fica na raiz do Volume.
def landing_incremental_dir(dataset: str) -> str:
    return f"{landing_volume_dir(dataset)}/incremental"


# Definição das tabelas
def bronze_table(dataset: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{dataset}"

def silver_table(dataset: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{dataset}"

def gold_table(dataset: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{dataset}"


# Espelhado no parametro schema => do read_files em src/dlt/01_bronze.sql.
BRONZE_SCHEMAS = {
    "business": StructType(
        [
            StructField("business_id", StringType()),
            StructField("name", StringType()),
            StructField("address", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("postal_code", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("stars", DoubleType()),
            StructField("review_count", LongType()),
            StructField("is_open", LongType()),
            StructField("attributes", StringType()),
            StructField("categories", StringType()),
            StructField("hours", StringType()),
        ]
    ),
    "review": StructType(
        [
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", DoubleType()),
            StructField("useful", LongType()),
            StructField("funny", LongType()),
            StructField("cool", LongType()),
            StructField("text", StringType()),
            StructField("date", TimestampType()),
        ]
    ),
    "user": StructType(
        [
            StructField("user_id", StringType()),
            StructField("name", StringType()),
            StructField("review_count", LongType()),
            StructField("yelping_since", TimestampType()),
            StructField("useful", LongType()),
            StructField("funny", LongType()),
            StructField("cool", LongType()),
            StructField("elite", StringType()),
            StructField("friends", StringType()),
            StructField("fans", LongType()),
            StructField("average_stars", DoubleType()),
            StructField("compliment_hot", LongType()),
            StructField("compliment_more", LongType()),
            StructField("compliment_profile", LongType()),
            StructField("compliment_cute", LongType()),
            StructField("compliment_list", LongType()),
            StructField("compliment_note", LongType()),
            StructField("compliment_plain", LongType()),
            StructField("compliment_cool", LongType()),
            StructField("compliment_funny", LongType()),
            StructField("compliment_writer", LongType()),
            StructField("compliment_photos", LongType()),
        ]
    ),
    "checkin": StructType(
        [
            StructField("business_id", StringType()),
            StructField("date", StringType()),
        ]
    ),
    "tip": StructType(
        [
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("text", StringType()),
            StructField("date", TimestampType()),
            StructField("compliment_count", LongType()),
        ]
    ),
}
