# processing/helpers/cleaning.py

from pyspark.sql import DataFrame, functions as F
from typing import Dict, List, Tuple

def clean_categorical_columns(df: DataFrame, invalid_values: Dict[str, List[str]]) -> DataFrame:
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

def clean_regex_columns(df: DataFrame, regex_cols: List[str]) -> DataFrame:
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

def mask_pii(df: DataFrame, col: str, patterns: List[Tuple[str, str]]) -> DataFrame:
    """
    Mask PII in a column using regex patterns.
    """
    for pattern, mask in patterns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), pattern, mask))
    return df
