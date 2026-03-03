from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("LeastPickupZone").getOrCreate()

# Read main dataset
df = spark.read.parquet("yel_nov25_repartitioned")

# Read zone lookup
zones = spark.read.csv("taxi_zone_lookup.csv", header=True)

# Create temp views
df.createOrReplaceTempView("yellow")
zones.createOrReplaceTempView("zones")

# Join trips with zones
joined = df.join(zones, df.PULocationID == zones.LocationID)

# Count trips per zone
counts = joined.groupBy("Zone").agg(count("*").alias("trip_count")).orderBy("trip_count")

counts.show(1)  # Shows the least frequent

spark.stop()