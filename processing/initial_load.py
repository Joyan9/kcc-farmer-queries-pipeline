# processing/initial_load.py

import logging
from pyspark.sql import SparkSession, functions as F
import duckdb
from config import (
    RAW_PARQUET_PATH, PROCESSED_DATA_DIR, DUCKDB_PATH,
    INVALID_VALUES, REGEX_INVALID_COLS, PII_PATTERNS, RAW_SCHEMA
)
from helpers.cleaning import clean_categorical_columns, clean_regex_columns, mask_pii
from helpers.dimensions import generate_dim
from helpers.io import write_parquet, duckdb_create_table_from_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main() -> None:
    spark = SparkSession.builder \
        .appName("KCC Initial Star Schema Build") \
        .config('spark.ui.port', '4040') \
        .getOrCreate()
    try:
        logger.info("Reading raw data with defined schema...")
        df = spark.read.option("header", True).schema(RAW_SCHEMA).parquet(RAW_PARQUET_PATH)

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
        from pyspark.sql.functions import broadcast
        fact = df_cleaned \
            .join(broadcast(dim_category), on="category", how="left") \
            .join(broadcast(dim_sector), on="sector", how="left") \
            .join(broadcast(dim_demography), on="state_name", how="left")

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
        write_parquet(fact_queries, f"{PROCESSED_DATA_DIR}/fct_queries")
        write_parquet(dim_category, f"{PROCESSED_DATA_DIR}/dim_category")
        write_parquet(dim_sector, f"{PROCESSED_DATA_DIR}/dim_sector")
        write_parquet(dim_demography, f"{PROCESSED_DATA_DIR}/dim_demography")

        logger.info("Loading tables into DuckDB...")
        conn = duckdb.connect(DUCKDB_PATH)
        duckdb_create_table_from_parquet(conn, "fct_queries", f"{PROCESSED_DATA_DIR}/fct_queries")
        duckdb_create_table_from_parquet(conn, "dim_category", f"{PROCESSED_DATA_DIR}/dim_category")
        duckdb_create_table_from_parquet(conn, "dim_sector", f"{PROCESSED_DATA_DIR}/dim_sector")
        duckdb_create_table_from_parquet(conn, "dim_demography", f"{PROCESSED_DATA_DIR}/dim_demography")

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
