-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables with SQL
-- MAGIC 
-- MAGIC This notebook uses SQL to declare Delta Live Tables. Note that this syntax is not intended for interactive execution in a notebook. Instructions for deploying this notebook are present in the [companion notebook]($./delta-live-tables).
-- MAGIC 
-- MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic DLT SQL Syntax
-- MAGIC 
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to tradtional CTAS statements.
-- MAGIC 
-- MAGIC DLT tables and views will always be preceded by the `LIVE` keyword.
-- MAGIC 
-- MAGIC If you wish to process data incrementally (using the same processing model as Structured Streaming), also use the `INCREMENTAL` keyword.
-- MAGIC 
-- MAGIC **NOTE**: The setup script run in the [companion notebook]($./delta-live-tables) uses your username to configure a directory for use in this demo. Make sure you enter this value in place of `<YOUR_USERNAME_HERE>` below.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE recordings_bronze
AS SELECT * FROM cloud_files("/user/<YOUR_USERNAME_HERE>/dlt/source", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auto Loader with SQL
-- MAGIC 
-- MAGIC Note that prior to DLT, Spark SQL did not have native support for incremental data loading; the `cloud_files` method has been added to allow Auto Loader to be used natively with SQL.
-- MAGIC 
-- MAGIC Here we use `cloud_files` to load data from a CSV. The third positional argument takes any number of reader options; here we're setting:
-- MAGIC 
-- MAGIC | option | value |
-- MAGIC | --- | --- |
-- MAGIC | `header` | `true` |
-- MAGIC | `cloudFiles.inferColumnTypes` | `true` |
-- MAGIC 
-- MAGIC Auto Loader configurations can be found [here](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html#configuration).
-- MAGIC 
-- MAGIC CSV options can be found [here](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv).

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW pii
AS SELECT * FROM cloud_files("/mnt/training/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Referencing Streaming Tables
-- MAGIC 
-- MAGIC Queries against other DLT tables and views will always use the syntax `live.table_name`. At execution, the target database name will be substituted, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC 
-- MAGIC When referring to another streaming DLT table within a pipeline, use the `STREAM(live.table_name)` syntax to ensure incremental processing.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE recordings_parsed
AS SELECT 
  CAST(device_id AS INTEGER) device_id, 
  CAST(mrn AS LONG) mrn, 
  CAST(heartrate AS DOUBLE) heartrate, 
  CAST(FROM_UNIXTIME(DOUBLE(time), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time 
FROM STREAM(live.recordings_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quality Control with Constraint Clauses
-- MAGIC 
-- MAGIC Data expectations are expressed as simple constraint clauses, which are essential where statements against a field in a table.
-- MAGIC 
-- MAGIC Adding a constraint clause will always collect metrics on violations. If no `ON VIOLATION` clause is included, records violating the expectation will still be included.
-- MAGIC 
-- MAGIC DLT currently supports two options for the `ON VIOLATION` clause.
-- MAGIC 
-- MAGIC | mode | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `FAIL UPDATE` | Fail when expectation is not met |
-- MAGIC | `DROP ROW` | Only process records that fulfill expectations |

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE recordings_enriched
  (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
AS SELECT device_id, a.mrn, name, time, heartrate
  FROM STREAM(live.recordings_parsed) a
  INNER JOIN STREAM(live.pii) b
  ON a.mrn = b.mrn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Configurations
-- MAGIC A number of additional configurations can be set at the table level, including partition columns and table properties. All of these configurations occur before the `AS` clause in the entity definition.
-- MAGIC 
-- MAGIC Here we pass a comment that will be visible to analysts exploring our data catalog.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE daily_patient_avg
COMMENT "Daily mean heartrates by patient"
AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
FROM STREAM(live.recordings_enriched)
GROUP BY mrn, name, DATE(time)
