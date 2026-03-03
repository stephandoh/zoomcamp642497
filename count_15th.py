from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("Count15Nov").getOrCreate()

df = spark.read.parquet("yel_nov25_repartitioned")

# Filter trips that started on 15th November 2025
df_15th = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15")

count_15th = df_15th.count()
print("Trips on 15th Nov:", count_15th)

spark.stop()