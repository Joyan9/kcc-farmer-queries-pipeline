import os
import logging
from pyspark.sql import SparkSession, functions as F
import duckdb
from helpers import (
    clean_categorical_columns,
    clean_regex_columns,
    mask_pii,
    generate_dim,
    write_parquet,
    duckdb_create_table_from_parquet
)

# ----------------- Config & Constants -----------------
RAW_PARQUET_PATH = "/app/storage/raw_data/kcc_data_*/data.parquet"
PROCESSED_DATA_DIR = "/app/storage/processed_data"
DUCKDB_PATH = os.path.join(PROCESSED_DATA_DIR, "kcc_queries_processed.duckdb")

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("KCC Initial Star Schema Build") \
        .config('spark.ui.port', '4040') \
        .getOrCreate()
    try:
        logger.info("Reading raw data...")
        df = spark.read.option("header", True).option("inferSchema", True).parquet(RAW_PARQUET_PATH)

        logger.info("Cleaning categorical columns...")
        df_cleaned = clean_categorical_columns(df, INVALID_VALUES)
        logger.info("Cleaning regex columns...")
        df_cleaned = clean_regex_columns(df_cleaned, REGEX_INVALID_COLS)
        logger.info("Removing unwanted characters in query_type...")
        df_cleaned = df_cleaned.withColumn("query_type", F.regexp_replace("query_type", r"\t", ""))

        logger.info("Masking PII in kcc_ans...")
        df_cleaned = mask_pii(df_cleaned, "kcc_ans", PII_PATTERNS)

        logger.info("Generating dimension tables...")
        dim_category = generate_dim(df_cleaned, "category", "category_id")
        dim_sector = generate_dim(df_cleaned, "sector", "sector_id")
        dim_state = generate_dim(df_cleaned, "state_name", "state_id")

        logger.info("Building dim_demography...")
        dim_demography = df_cleaned.groupBy("state_name").agg(
            F.collect_set("district_name").alias("district_names"),
            F.collect_set("block_name").alias("block_names")
        ).join(dim_state, on="state_name", how="left") \
         .select("state_id", "state_name", "district_names", "block_names")

        logger.info("Building fact table...")
        fact = df_cleaned \
            .join(dim_category, on="category", how="left") \
            .join(dim_sector, on="sector", how="left") \
            .join(dim_demography, on="state_name", how="left")

        fact_queries = fact.select(
            F.col("_dlt_id").alias("query_id"),
            "created_on",
            "state_id",
            "category_id",
            "sector_id",
            "crop",
            "query_type",
            "query_text",
            "kcc_ans"
        )

        logger.info("Writing tables as Parquet...")
        write_parquet(fact_queries, os.path.join(PROCESSED_DATA_DIR, "fct_queries"))
        write_parquet(dim_category, os.path.join(PROCESSED_DATA_DIR, "dim_category"))
        write_parquet(dim_sector, os.path.join(PROCESSED_DATA_DIR, "dim_sector"))
        write_parquet(dim_demography, os.path.join(PROCESSED_DATA_DIR, "dim_demography"))

        logger.info("Loading tables into DuckDB...")
        conn = duckdb.connect(DUCKDB_PATH)
        duckdb_create_table_from_parquet(conn, "fct_queries", os.path.join(PROCESSED_DATA_DIR, "fct_queries"))
        duckdb_create_table_from_parquet(conn, "dim_category", os.path.join(PROCESSED_DATA_DIR, "dim_category"))
        duckdb_create_table_from_parquet(conn, "dim_sector", os.path.join(PROCESSED_DATA_DIR, "dim_sector"))
        duckdb_create_table_from_parquet(conn, "dim_demography", os.path.join(PROCESSED_DATA_DIR, "dim_demography"))

        logger.info("Tables created in DuckDB:")
        for table in conn.execute("SHOW TABLES").fetchall():
            logger.info(f"- {table[0]}")
        conn.close()
        logger.info("Initial star schema build complete!")
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
