# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables with Python
# MAGIC 
# MAGIC This notebook uses Python to declare Delta Live Tables. Note that this syntax is not intended for interactive execution in a notebook. Instructions for deploying this notebook are present in the [companion notebook]($./delta-live-tables).
# MAGIC 
# MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#python).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports
# MAGIC It's necessary to import the `dlt` Python module to use the associated methods.
# MAGIC 
# MAGIC Here, we also import and alias `pyspark.sql.functions`.
# MAGIC 
# MAGIC The setup script run in the [companion notebook]($./delta-live-tables) uses your username to configure a directory for use in this demo. Make sure you set this variable in the cell below.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

username = "<YOUR_USERNAME_HERE>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic DLT Python Syntax
# MAGIC 
# MAGIC DLT tables, views, and their associated settings are configured using [decorators](https://www.python.org/dev/peps/pep-0318/#current-syntax).
# MAGIC 
# MAGIC If you're unfamiliar with Python decorators, just note that they are functions or classes preceded with the `@` sign that interact with the next function present in a Python script.
# MAGIC 
# MAGIC The `@dlt.table` decorator is the basic method for turning a Python function into a Delta Live Table.
# MAGIC 
# MAGIC The function must return a PySpark or Koalas DataFrame. Here we're using [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) to incrementally ingest files from object storage.

# COMMAND ----------

@dlt.table(name="recordings_bronze")
def recordings_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("device_id STRING, mrn STRING, heartrate STRING, time STRING")
        .load(f"/user/{username}/dlt/source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Delta Live Tables
# MAGIC 
# MAGIC Our initial ingestion table is defined using a tradtional `spark.readStream` query.
# MAGIC 
# MAGIC The `dlt` module contains the `read_stream` method, which accepts the name of another DLT table or view as a string.
# MAGIC 
# MAGIC Note that when the optional keyword argument `name` is not passed to `@dlt.table`, the name of the function will become the table name.

# COMMAND ----------

@dlt.table
def recordings_parsed():
    return (dlt.read_stream("recordings_bronze")
        .select(F.col("device_id").cast("integer").alias("device_id"), 
            F.col("mrn").cast("long").alias("mrn"), 
            F.col("heartrate").cast("double").alias("heartrate"), 
            F.from_unixtime(F.col("time").cast("double"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").alias("time"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Views
# MAGIC 
# MAGIC Use `@dlt.view` when you want to register a query that will be executed at runtime.
# MAGIC 
# MAGIC Here, we expect the CSV we're reading to not change, but our logic will execute against the source file/directory each time a pipeline update is executed.
# MAGIC 
# MAGIC We'll later refer to this view with `@dlt.read("pii")` to configure a complete (rather than incremental) read of its contents.

# COMMAND ----------

@dlt.view
def pii():
    return (spark
        .read
        .format("csv")
        .schema("mrn STRING, name STRING")
        .option("header", True)
        .load("/mnt/training/healthcare/patient/patient_info.csv")
    ) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Control with Expectations
# MAGIC 
# MAGIC Data expectations are expressed as simple filters against a field in a table.
# MAGIC 
# MAGIC DLT currently supports three modes for expectations:
# MAGIC 
# MAGIC | mode | behavior |
# MAGIC | --- | --- |
# MAGIC | `@dlt.expect` | Record metrics for percentage of records that fulfill expectation <br> (**NOTE**: this metric is reported for all execution modes) |
# MAGIC | `@dlt.expect_or_fail` | Fail when expectation is not met |
# MAGIC | `@dlt.expect_or_drop` | Only process records that fulfill expectations |

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("positive_heartrate", "heartrate > 0")
def recordings_enriched():
    return (dlt.read_stream("recordings_parsed")
        .join(dlt.read("pii"), ["mrn"], "inner")
        .select("device_id", "mrn", "name", "time", "heartrate")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Configurations
# MAGIC A number of additional configurations can be set at the table level, including partition columns and Spark configurations.
# MAGIC 
# MAGIC Here we pass a comment that will be visible to analysts exploring our data catalog.

# COMMAND ----------

@dlt.table(comment="Daily mean heartrates by patient")
def daily_patient_avg():
    return (dlt.read_stream("recordings_enriched")
        .groupBy("mrn", "name", F.date_trunc("DD", "time").alias("date"))
        .agg(F.mean("heartrate").alias("avg_heartrate"))
        .select("mrn", "name", "avg_heartrate", "date")
    )
