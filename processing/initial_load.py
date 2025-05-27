# processing/initial_load.py

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
import duckdb

from config import (
    PROCESSED_DATA_DIR, DUCKDB_PATH,
    INVALID_VALUES, REGEX_INVALID_COLS, PII_PATTERNS, RAW_SCHEMA
)
from helpers.cleaning import clean_categorical_columns, clean_regex_columns, mask_pii
from helpers.dimensions import generate_dim
from helpers.io import write_parquet, duckdb_create_table_from_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_parquet_file_paths(base_dir, start_year=2018, end_year=None):
    """
    Returns a list of parquet file paths from start_year to end_year (inclusive).
    If end_year is None, uses current year and month.
    """
    file_paths = []
    now = datetime.now()
    if end_year is None:
        end_year = now.year
        end_month = now.month
    else:
        end_month = 12

    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            # Don't include future months
            if year == now.year and month > now.month:
                break
            path = f"{base_dir}/kcc_data_{year}_{month:02d}/data.parquet"
            if os.path.exists(path):
                file_paths.append(path)
    return file_paths

def main() -> None:
    spark = SparkSession.builder \
        .appName("KCC Initial Star Schema Build") \
        .config('spark.ui.port', '4040') \
        .getOrCreate()
    try:
        BASE_RAW_DIR = "/app/storage/raw_data"
        file_paths = get_parquet_file_paths(BASE_RAW_DIR, start_year=2018)
        if not file_paths:
            logger.error("No parquet files found in the specified range.")
            return

        logger.info(f"Reading {len(file_paths)} raw data files from 2018 onward...")
        df = spark.read.option("header", True).schema(RAW_SCHEMA).parquet(*file_paths)

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
