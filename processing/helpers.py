from pyspark.sql import functions as F, Window

def clean_categorical_columns(df, invalid_values):
    """
    Replace nulls and invalids in categorical columns.
    """
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
    """
    Replace numeric/NA/0/null in specified columns.
    """
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
    """
    Mask PII in a column using regex patterns.
    """
    for pattern, mask in patterns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), pattern, mask))
    return df

def generate_dim(df, col, id_col):
    """
    Generate a dimension table with surrogate keys.
    """
    window = Window.orderBy(col)
    return df.select(col).distinct().withColumn(id_col, F.row_number().over(window))

def write_parquet(df, path):
    """
    Write a DataFrame to Parquet (overwrite mode).
    """
    df.write.mode("overwrite").parquet(path)

def duckdb_create_table_from_parquet(conn, table_name, parquet_path):
    """
    Create or replace a DuckDB table from a Parquet file.
    """
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM parquet_scan('{parquet_path}/*.parquet')
    """)
