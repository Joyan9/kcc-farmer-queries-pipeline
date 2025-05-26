# processing/helpers/io.py

from pyspark.sql import DataFrame
import duckdb

def write_parquet(df: DataFrame, path: str) -> None:
    """
    Write a DataFrame to Parquet (overwrite mode).
    """
    df.write.mode("overwrite").parquet(path)

def duckdb_create_table_from_parquet(conn: duckdb.DuckDBPyConnection, table_name: str, parquet_path: str) -> None:
    """
    Create or replace a DuckDB table from a Parquet file.
    """
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM parquet_scan('{parquet_path}/*.parquet')
    """)

def ensure_duckdb_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure all required DuckDB tables exist (idempotent DDL).
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_category (
        category_id INTEGER PRIMARY KEY,
        category VARCHAR
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_sector (
        sector_id INTEGER PRIMARY KEY,
        sector VARCHAR
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_demography (
        state_id INTEGER PRIMARY KEY,
        state_name VARCHAR,
        district_names VARCHAR[],
        block_names VARCHAR[]
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS fct_queries (
        query_id VARCHAR PRIMARY KEY,
        created_on TIMESTAMP,
        state_id INTEGER,
        category_id INTEGER,
        sector_id INTEGER,
        crop VARCHAR,
        query_type VARCHAR,
        query_text TEXT,
        kcc_ans TEXT
    );
    """)
