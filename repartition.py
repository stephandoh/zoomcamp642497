from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YelNov25").getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

#put in 4 partitions

df = df.repartition(4)

df.write.mode("overwrite").parquet("yel_nov25_repartitioned")

spark.stop()