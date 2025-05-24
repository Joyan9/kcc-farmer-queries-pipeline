import os
import logging
from pyspark.sql import SparkSession, functions as F, Window
import duckdb

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

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------- Helper Functions -----------------
def clean_categorical_columns(df, invalid_values):
    """Replace nulls and invalids in categorical columns."""
    for col, invalids in invalid_values.items():
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isin(invalids) | F.col(col).isNull(),
                "Not Available"
            ).otherwise(F.col(col))
        )
    return df

def clean_regex_columns(df, regex_cols):
    """Replace numeric/NA/0/null in specified columns."""
    for col in regex_cols:
        df = df.withColumn(
            col,
            F.when(
                F.col(col).rlike("^[0-9]+$") | 
                F.col(col).isin("NA", "0") | 
                F.col(col).isNull(),
                "Not Available"
            ).otherwise(F.col(col))
        )
    return df

def mask_pii(df, col, patterns):
    """Mask PII in a column using regex patterns."""
    for pattern, mask in patterns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), pattern, mask))
    return df

def generate_dim(df, col, id_col):
    """Generate a dimension table with surrogate keys."""
    window = Window.orderBy(col)
    return df.select(col).distinct().withColumn(id_col, F.row_number().over(window))

def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def duckdb_create_table_from_parquet(conn, table_name, parquet_path):
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM parquet_scan('{parquet_path}/*.parquet')
    """)

# ----------------- Main ETL Logic -----------------
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
