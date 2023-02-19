# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader Schema Inference & Evolution
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Starting in DBR 8.2, Databricks Auto Loader has introduced support for schema inference and evolution using Auto Loader. In this notebook, we'll explore a number of potential use cases for this new set of features.
# MAGIC 
# MAGIC Note that this functionality is limited to JSON, binary, and text file formats. In this notebook, we'll only be exploring options for working with JSON data.
# MAGIC 
# MAGIC Throughout this demo, we'll use "directory listing mode"; if you wish to use ["file notification mode" for file discovery](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html#file-discovery-modes), you'll need to configure additional options.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you will be able to use Auto Loader to achieve several common ingestion strategies. These include:
# MAGIC * Ingest data to Delta Lake without losing any data
# MAGIC * Rescue unexpected data in well-structured data
# MAGIC 
# MAGIC 
# MAGIC For detailed reference on Auto Loader and these new features, review:
# MAGIC * [Auto Loader Documentation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html)
# MAGIC * [Schema Inference & Evolution in Auto Loader Documentation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html)

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell resets our demo environment and defines paths and variables that will be used in the demo. A Python class is also defined that we'll use later to create and load additional fake data.

# COMMAND ----------

# MAGIC %run ./Includes/autoloader-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our Recommendation for Easy ETL
# MAGIC 
# MAGIC 
# MAGIC To enable schema inference and evolution, the following conditions need to be met:
# MAGIC 1. Do **not** provide a schema for your data
# MAGIC 1. Provide a location to store your inferred schemas using the `"cloudFiles.schemaLocation"` option in your DataStreamReader
# MAGIC   * Here we show using the checkpoint path, which is recommended
# MAGIC 1. Set the option `"mergeSchema"` to `True` in your DataStreamWriter.

# COMMAND ----------

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", bronzeCheckpoint)
    .load(rawDataSource)
    .writeStream
    .option("path", bronzeTable)
    .option("checkpointLocation", bronzeCheckpoint)
    .option("mergeSchema", True)
    .table(bronzeTableName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC A helper class was defined in our setup script to demo landing new files. Each time you execute the cell below, a new file will be written to our source directory. Expand the stream monitor above to see this data processing.

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, our stream is working. Without even looking at our data, we successfully configured data ingestion to automatically process new files as they arrive.
# MAGIC 
# MAGIC Let's display our ingested data to confirm it looks okay.

# COMMAND ----------

display(spark.table(bronzeTableName))

# COMMAND ----------

# MAGIC %md
# MAGIC As a frame of reference, we can look at the beginning of one of our JSON source files.

# COMMAND ----------

dbutils.fs.head(rawDataSource + "/file-0.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that there appear to only be 4 fields present, with a mix of integer, float, and string types.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inferring Strings and Rescuing Data
# MAGIC 
# MAGIC The `_rescued_data` column is added to the schema by default whenever Auto Loader infers the schema. This computed field is intended to capture any data that cannot be parsed due to
# MAGIC * type mismatch
# MAGIC * column casing mismatch (assuming case sensitivity has been activated)
# MAGIC * omission from the current schema (assuming schema evolution is not active)
# MAGIC 
# MAGIC The default configuration for schema inference with Auto Loader is _very_ permissive. To avoid potential conflicts as data change in the future (integers becoming floats, numeric IDs becoming alphanumeric), Auto Loader infers _all_ fields as strings. (We will explore additional options later in this demo.)
# MAGIC 
# MAGIC Check the table description to confirm.

# COMMAND ----------

display(spark.sql(f"DESCRIBE FORMATTED {bronzeTableName}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that we set our schema location to match our checkpoint path. A `_schemas` directory will now exist here.

# COMMAND ----------

dbutils.fs.ls(bronzeCheckpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC This directory stores each version of our inferred or evolved schema to save time on stream restart.
# MAGIC 
# MAGIC In this example, schema inference was very fast because of the small amount of data being used. By default during an initial read, Auto Loader will sample the first 50 GB or 1000 files (whichever limit is crossed first) to infer the schema of a dataset. As the schema evolves, this location will always serve as the single-source-of-truth.

# COMMAND ----------

dbutils.fs.ls(bronzeCheckpoint + "/_schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### When a New Column Appears...
# MAGIC 
# MAGIC Schema evolution for Auto Loader was designed to take advantage of recommended best practices when deploying Structured Streaming jobs on Databricks. Specifically, this feature was built around [deploying streams as production jobs configured to restart queries on failure](https://docs.databricks.com/spark/latest/structured-streaming/production.html#configure-jobs-to-restart-streaming-queries-on-failure).
# MAGIC 
# MAGIC When a new column appears in the data being processed, Spark will throw a `UnknownFieldException`, causing the job to fail.
# MAGIC 
# MAGIC The default setting for `"cloudFiles.schemaEvolutionMode"` is `addNewColumns`. This means that on job restart, the new fields that caused job failure will automatically be added to the schema.
# MAGIC 
# MAGIC ### ... Expect Job Failure
# MAGIC 
# MAGIC Executing the cell below will write a file with an additional field; the stream above should fail with the following error:
# MAGIC ```
# MAGIC UnknownFieldException: Encountered unknown field(s) during parsing: {"metadata":{"version":"1.2.3"}}
# MAGIC ```

# COMMAND ----------

File.newData(new_field=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the schema updates as the job fails, with the `metadata` field added to our schema as a `STRING` type.

# COMMAND ----------

dbutils.fs.ls(bronzeCheckpoint + "/_schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart the Stream
# MAGIC 
# MAGIC The code below is identical to our previous streaming query. (Here, we duplicate the code for easy cell-by-cell navigation of the demo.)

# COMMAND ----------

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", bronzeCheckpoint)
    .load(rawDataSource)
    .writeStream
    .option("path", bronzeTable)
    .option("checkpointLocation", bronzeCheckpoint)
    .option("mergeSchema", True)
    .table(bronzeTableName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Display the table to validate the `metadata` field has been added.

# COMMAND ----------

display(spark.table(bronzeTableName).orderBy(F.desc("timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Violations with Default Schema Inference
# MAGIC Because all of our fields have been inferred as strings, we're counting on enforcing quality checks on our data at a later stage.
# MAGIC 
# MAGIC Our current timestamp encodes milliseconds as a `LONG`. The cell below will write out Unix time as seconds since epoch as a float.

# COMMAND ----------

File.newData(bad_timestamp=True)

# COMMAND ----------

# MAGIC %md
# MAGIC The above data should not have caused your stream to fail; run the display query below to see that this incorrect timestamp has been ingested.

# COMMAND ----------

display(spark.table(bronzeTableName).orderBy(F.asc("timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Hints and Rescued Data
# MAGIC 
# MAGIC The new option `"cloudFiles.schemaHints"` allows users to provide field names and types that are expected in the data while still allowing for schema inference and evolution.
# MAGIC 
# MAGIC This is useful when you'd like to enforce type expectations for some fields, but still have the flexibility to evolve schema over time.
# MAGIC 
# MAGIC We'll reset our tables and source data to demonstrate this alternate behavior.

# COMMAND ----------

# MAGIC %run ./Includes/autoloader-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we're providing schema hints for three fields:
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | signalStrength | `FLOAT` |
# MAGIC | timestamp | `LONG` |
# MAGIC | metadata | `MAP<STRING, STRING>` |
# MAGIC 
# MAGIC All non-specified fields, inferred fields, and the `_rescued_data` column will still be inferred as string type.
# MAGIC 
# MAGIC Here, we provide a hint for `metadata`, even though this field will not be in our initial read of the data. Any field provided in `schemaHints` will be written to our bronze table, which gives us flexibility to avoid stream failure and restart if we know which new fields will be added to our data.

# COMMAND ----------

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "signalStrength FLOAT, timestamp LONG, metadata MAP<STRING, STRING>")
    .option("cloudFiles.schemaLocation", bronzeCheckpoint)
    .load(rawDataSource)
    .writeStream
    .option("path", bronzeTable)
    .option("checkpointLocation", bronzeCheckpoint)
    .option("mergeSchema", True)
    .table(bronzeTableName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our expected schema has been used to create our bronze table.

# COMMAND ----------

display(spark.sql(f"DESCRIBE FORMATTED {bronzeTableName}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now when an improper timestamp arrives, we will quarantine this data to the `_rescued_data` column.

# COMMAND ----------

File.newData(bad_timestamp=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We can find these records by their `NULL` timestamps.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {bronzeTableName} ORDER BY timestamp DESC NULLS FIRST"))

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below will add a file that contains `metadata`. This new field will be processed with our previously defined schema hint (rather than as a `STRING`).

# COMMAND ----------

File.newData(new_field=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Rather than failing on a new field, our stream is still active. Note how casting this as `map<string, string>` allows us to interact with the nested fields.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {bronzeTableName} WHERE metadata IS NOT NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Using New Support for Semi-Structured Data
# MAGIC Databricks recently introduced [support for querying semi-structured JSON data stored as strings](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html). 
# MAGIC 
# MAGIC This `:` operator is extremely useful for dealing with `_rescued_data`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT _rescued_data, _rescued_data:timestamp, BIGINT(_rescued_data:timestamp * 1000) AS repaired_timestamp
    FROM {bronzeTableName} 
    WHERE _rescued_data IS NOT NULL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see in the example above, this allows us to easily write logic to repair data that would otherwise have been lost by schema enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all active streams before continuing.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
