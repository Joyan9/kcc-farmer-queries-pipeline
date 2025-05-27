# processing/incremental_load.py

import os
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as T

from config import (
    PROCESSED_DATA_DIR, DUCKDB_PATH, INVALID_VALUES, REGEX_INVALID_COLS,
    PII_PATTERNS, RAW_SCHEMA
)
from helpers.cleaning import clean_categorical_columns, clean_regex_columns, mask_pii
from helpers.io import ensure_duckdb_tables
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_last_month_file() -> str:
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    year = last_month.year
    month = f"{last_month.month:02d}"
    file_path = f"/app/storage/raw_data/kcc_data_{year}_{month}/data.parquet"
    return file_path

def main() -> None:
    spark = SparkSession.builder.appName("KCC Incremental Load").getOrCreate()
    try:
        file_path = get_last_month_file()
        logger.info(f"Looking for file: {file_path}")
        if not os.path.exists(file_path):
            logger.warning("No new data file found for last month.")
            return

        df = spark.read.option("header", True).schema(RAW_SCHEMA).parquet(file_path)
        logger.info(f"Read {df.count()} rows from {file_path}")

        df_cleaned = clean_categorical_columns(df, INVALID_VALUES)
        df_cleaned = clean_regex_columns(df_cleaned, REGEX_INVALID_COLS)
        df_cleaned = df_cleaned.withColumn("query_type", F.regexp_replace("query_type", r"\t", ""))
        df_cleaned = mask_pii(df_cleaned, "kcc_ans", PII_PATTERNS)
        logger.info(f"Rows after cleaning: {df_cleaned.count()}")

        # Load dims from DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        ensure_duckdb_tables(conn)

        dim_category_pd = conn.execute("SELECT * FROM dim_category").df()
        dim_sector_pd = conn.execute("SELECT * FROM dim_sector").df()
        dim_demography_pd = conn.execute("SELECT * FROM dim_demography").df()
        
        CATEGORY_SCHEMA = T.StructType([
            T.StructField("category_id", T.IntegerType(), False),
            T.StructField("category", T.StringType(), False),
        ])

        SECTOR_SCHEMA = T.StructType([
            T.StructField("sector_id", T.IntegerType(), False),
            T.StructField("sector", T.StringType(), False),
        ])

        DEMOGRAPHY_SCHEMA = T.StructType([
            T.StructField("state_id", T.IntegerType(), False),
            T.StructField("state_name", T.StringType(), False),
            T.StructField("district_names", T.ArrayType(T.StringType()), True),
            T.StructField("block_names", T.ArrayType(T.StringType()), True),
        ])

        # Category
        if dim_category_pd.empty:
            dim_category_spark = spark.createDataFrame([], schema=CATEGORY_SCHEMA)
        else:
            dim_category_spark = spark.createDataFrame(dim_category_pd, schema=CATEGORY_SCHEMA)

        # Sector
        if dim_sector_pd.empty:
            dim_sector_spark = spark.createDataFrame([], schema=SECTOR_SCHEMA)
        else:
            dim_sector_spark = spark.createDataFrame(dim_sector_pd, schema=SECTOR_SCHEMA)

        # Demography
        if dim_demography_pd.empty:
            dim_demography_spark = spark.createDataFrame([], schema=DEMOGRAPHY_SCHEMA)
        else:
            # Convert lists to arrays in Pandas
            dim_demography_pd["district_names"] = dim_demography_pd["district_names"].apply(lambda x: x if isinstance(x, list) else [])
            dim_demography_pd["block_names"] = dim_demography_pd["block_names"].apply(lambda x: x if isinstance(x, list) else [])
            dim_demography_spark = spark.createDataFrame(dim_demography_pd, schema=DEMOGRAPHY_SCHEMA)


        from pyspark.sql.functions import broadcast

        # Assign surrogate keys (reuse or add new)
        # Category
        new_categories = df_cleaned.select("category").distinct().subtract(dim_category_spark.select("category"))
        if new_categories.count() > 0:
            max_cat_id = dim_category_pd["category_id"].max() if not dim_category_pd.empty else 0
            # Use row_number for deterministic IDs
            from pyspark.sql.window import Window
            window = Window.orderBy("category")
            new_categories = new_categories.withColumn(
                "category_id", F.row_number().over(window) + max_cat_id
            )
            dim_category_spark = dim_category_spark.unionByName(new_categories)
            # Update DuckDB
            conn.execute("DELETE FROM dim_category")
            conn.register("dim_category_spark", dim_category_spark.toPandas())
            conn.execute("INSERT INTO dim_category SELECT * FROM dim_category_spark")
            logger.info(f"Added {new_categories.count()} new categories.")

        # Sector
        new_sectors = df_cleaned.select("sector").distinct().subtract(dim_sector_spark.select("sector"))
        if new_sectors.count() > 0:
            max_sector_id = dim_sector_pd["sector_id"].max() if not dim_sector_pd.empty else 0
            window = Window.orderBy("sector")
            new_sectors = new_sectors.withColumn(
                "sector_id", F.row_number().over(window) + max_sector_id
            )
            dim_sector_spark = dim_sector_spark.unionByName(new_sectors)
            conn.execute("DELETE FROM dim_sector")
            conn.register("dim_sector_spark", dim_sector_spark.toPandas())
            conn.execute("INSERT INTO dim_sector SELECT * FROM dim_sector_spark")
            logger.info(f"Added {new_sectors.count()} new sectors.")

        # Demography (state_name)
        new_states = df_cleaned.select("state_name").distinct().subtract(dim_demography_spark.select("state_name"))
        if new_states.count() > 0:
            max_state_id = dim_demography_pd["state_id"].max() if not dim_demography_pd.empty else 0
            window = Window.orderBy("state_name")
            new_states = new_states.withColumn(
                "state_id", F.row_number().over(window) + max_state_id
            )
            # For new states, collect district/block names
            new_demography = df_cleaned.join(new_states, on="state_name", how="inner") \
                .groupBy("state_name", "state_id") \
                .agg(
                    F.collect_set("district_name").alias("district_names"),
                    F.collect_set("block_name").alias("block_names")
                )
            dim_demography_spark = dim_demography_spark.unionByName(new_demography)
            conn.execute("DELETE FROM dim_demography")
            conn.register("dim_demography_spark", dim_demography_spark.toPandas())
            conn.execute("INSERT INTO dim_demography SELECT * FROM dim_demography_spark")
            logger.info(f"Added {new_states.count()} new states to demography.")

        # Join cleaned data with dims to assign surrogate keys
        fact = df_cleaned \
            .join(broadcast(dim_category_spark), on="category", how="left") \
            .join(broadcast(dim_sector_spark), on="sector", how="left") \
            .join(broadcast(dim_demography_spark), on="state_name", how="left")

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

        logger.info(f"Writing {fact_queries.count()} new fact rows to Parquet...")
        fact_queries.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_DIR, "fct_queries_incremental"))

        logger.info("Appending new fact rows to DuckDB...")
        conn.execute("""
            INSERT INTO fct_queries
            SELECT * FROM parquet_scan('/app/storage/processed_data/fct_queries_incremental/*.parquet')
        """)
        logger.info("Incremental load complete.")
        conn.close()
    except Exception as e:
        logger.exception(f"Incremental ETL job failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
