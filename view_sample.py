from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("YelNov25").getOrCreate()
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.show(5)
df.printSchema()