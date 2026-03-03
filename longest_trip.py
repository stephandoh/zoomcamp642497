from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

spark = SparkSession.builder.appName("LongestTrip").getOrCreate()

df = spark.read.parquet("yel_nov25_repartitioned")

# Compute trip duration in hours
df = df.withColumn(
    "trip_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)

max_hours = df.agg({"trip_hours": "max"}).collect()[0][0]
print("Longest trip in hours:", max_hours)

spark.stop()