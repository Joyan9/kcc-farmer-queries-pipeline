import os
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from helpers import clean_categorical_columns, clean_regex_columns, mask_pii

# Config
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_last_month_file():
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    year = last_month.year
    month = f"{last_month.month:02d}"
    file_path = f"/app/storage/raw_data/kcc_data_{year}_{month}/data.parquet"
    return file_path

def main():
    spark = SparkSession.builder.appName("KCC Incremental Load").getOrCreate()
    try:
        file_path = get_last_month_file()
        logger.info(f"Looking for file: {file_path}")
        if not os.path.exists(file_path):
            logger.warning("No new data file found for last month.")
            return

        df = spark.read.option("header", True).option("inferSchema", True).parquet(file_path)
        df_cleaned = clean_categorical_columns(df, INVALID_VALUES)
        df_cleaned = clean_regex_columns(df_cleaned, REGEX_INVALID_COLS)
        df_cleaned = df_cleaned.withColumn("query_type", F.regexp_replace("query_type", r"\t", ""))
        df_cleaned = mask_pii(df_cleaned, "kcc_ans", PII_PATTERNS)

        # Load dims from DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        dim_category_pd = conn.execute("SELECT * FROM dim_category").df()
        dim_sector_pd = conn.execute("SELECT * FROM dim_sector").df()
        dim_demography_pd = conn.execute("SELECT * FROM dim_demography").df()

        # Broadcast dims for joining in Spark
        dim_category_spark = spark.createDataFrame(dim_category_pd)
        dim_sector_spark = spark.createDataFrame(dim_sector_pd)
        dim_demography_spark = spark.createDataFrame(dim_demography_pd)

        # Assign surrogate keys (reuse or add new)
        # Category
        new_categories = df_cleaned.select("category").distinct().subtract(dim_category_spark.select("category"))
        if new_categories.count() > 0:
            max_cat_id = dim_category_pd["category_id"].max() if not dim_category_pd.empty else 0
            new_categories = new_categories.withColumn(
                "category_id", F.monotonically_increasing_id() + max_cat_id + 1
            )
            dim_category_spark = dim_category_spark.unionByName(new_categories)
            # Update DuckDB
            conn.execute("DELETE FROM dim_category")
            conn.register("dim_category_spark", dim_category_spark.toPandas())
            conn.execute("INSERT INTO dim_category SELECT * FROM dim_category_spark")

        # Repeat similar logic for sector and demography/state as needed...

        # Join cleaned data with dims to assign surrogate keys
        fact = df_cleaned \
            .join(dim_category_spark, on="category", how="left") \
            .join(dim_sector_spark, on="sector", how="left") \
            .join(dim_demography_spark, on="state_name", how="left")

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

        # Write new fact rows to Parquet
        fact_queries.write.mode("overwrite").parquet(os.path.join(PROCESSED_DATA_DIR, "fct_queries_incremental"))

        # Append to DuckDB
        conn.execute("""
            INSERT INTO fct_queries
            SELECT * FROM parquet_scan('/app/storage/processed_data/fct_queries_incremental/*.parquet')
        """)
        logger.info("Incremental load complete.")
        conn.close()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
