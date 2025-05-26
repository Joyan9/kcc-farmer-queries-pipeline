# processing/helpers/dimensions.py

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def generate_dim(df: DataFrame, col: str, id_col: str) -> DataFrame:
    """
    Generate a dimension table with surrogate keys.
    """
    window = Window.orderBy(col)
    return df.select(col).distinct().withColumn(id_col, F.row_number().over(window))
