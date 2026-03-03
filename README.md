### Building a Batch Pipeline for NYC Taxi Data with Spark

#### Overview

This project demonstrates a batch data pipeline using PySpark to analyze NYC Yellow Taxi trips for November 2025. The workflow covers reading Parquet data, repartitioning for parallel processing, performing aggregations, and joining with taxi zone information.

#### Requirements

- Windows 10 or 11

- Git Bash (MINGW)

- Java 17

- Python 3.x

- PySpark 4.x (bundled with Spark)

#### How to Run

- Install PySpark using either pip or uv.

- Start a local Spark session.

- Load the November 2025 Yellow Taxi Parquet dataset.

- Perform basic batch operations: repartitioning, filtering, aggregations, and joining with taxi zones.

- Monitor the job using the Spark UI at localhost:4040.

#### Key Features

- Reads static Parquet data efficiently

- Repartitions data for optimized batch processing

- Supports aggregation and filtering operations

- Integrates taxi zone lookup for enrichment
