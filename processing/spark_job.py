from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KCC_Pipeline").getOrCreate()

print("Hello World, spark imported")

# file path for all raw data parquet files
file_path = "/app/storage/raw_data/kcc_data_*.parquet"

# read all parquet files as a single dataframe
df = spark.read.option("header", True).parquet(file_path)
df.printSchema()
df.show(truncate=False)

# stage 1 - data cleaning
# column casting, column names standardising if required
# filling up Null values with placeholders such they can be grouped later
# stage 2 - data transformation
# create star schema: 
# fact_queries - query_id (surrogate key needs to be generated), state_id, district_id, category_id, crop_id, query_text, query_answer