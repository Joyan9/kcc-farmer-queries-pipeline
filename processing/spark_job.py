from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KCC_Pipeline").getOrCreate()

print("Hello World, spark imported")

file_path = "/app/storage/raw_data/kcc_data_2025_04/1747746883.8694966.37520cbae1.parquet"

df = spark.read.option("header", True).parquet(file_path)
df.printSchema()
df.show(truncate=False)
