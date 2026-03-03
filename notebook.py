import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("Taxi Sample") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

    df.limit(5).toPandas()
    return


if __name__ == "__main__":
    app.run()
