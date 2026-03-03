from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Module6Homework").getOrCreate()

print("Spark version:", spark.version)

spark.stop()