# processing/config.py

RAW_PARQUET_PATH = "/app/storage/raw_data/kcc_data_*/data.parquet"
PROCESSED_DATA_DIR = "/app/storage/processed_data"
DUCKDB_PATH = f"{PROCESSED_DATA_DIR}/kcc_queries_processed.duckdb"

INVALID_VALUES = {
    "state_name": ["NA", "0"],
    "district_name": ["NA", "9999"],
    "block_name": ["NA", "0   "],
    "category": ["0"],
    "season": ["NA"]
}
REGEX_INVALID_COLS = ["sector", "crop", "query_type", "category"]
PII_PATTERNS = [
    (r"(\+91[\-\s]?\d{10})|(\b\d{10}\b)", "[PHONE]"),
    (r"[a-zA-Z0-9.\-_]+@[a-zA-Z0-9\-_]+\.[a-zA-Z.]+", "[EMAIL]"),
    (r"\b\d{9,18}\b", "[ACCOUNT]")
]

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

RAW_SCHEMA = StructType([
    StructField("state_name", StringType(), True),
    StructField("district_name", StringType(), True),
    StructField("block_name", StringType(), True),
    StructField("season", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("category", StringType(), True),
    StructField("crop", StringType(), True),
    StructField("query_type", StringType(), True),
    StructField("query_text", StringType(), True),
    StructField("kcc_ans", StringType(), True),
    StructField("created_on", TimestampType(), True),
    StructField("year", LongType(), True),
    StructField("month", LongType(), True),
    StructField("_dlt_load_id", StringType(), True),
    StructField("_dlt_id", StringType(), True),
])
