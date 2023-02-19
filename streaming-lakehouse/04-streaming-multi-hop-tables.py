# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Multi-Hop Tables in the Lakehouse
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Discuss how Delta Lake evolves the traditional Lambda architecture
# MAGIC * Describe Bronze, Silver, and Gold tables
# MAGIC * Create a Delta Lake multi-hop pipeline
# MAGIC 
# MAGIC This notebook demonstrates using Delta Lakes as an optimization layer on top of cloud-based object storage to ensure reliability (i.e. ACID compliance) and low latency within unified Streaming + Batch data pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lambda Architecture
# MAGIC 
# MAGIC The Lambda architecture is a big data processing architecture that combines both batch- and real-time processing methods.
# MAGIC It features an append-only immutable data source that serves as system of record. Timestamped events are appended to
# MAGIC existing events (nothing is overwritten). Data is implicitly ordered by time of arrival.
# MAGIC 
# MAGIC Notice how there are really two pipelines here, one batch and one streaming, hence the name <i>lambda</i> architecture.
# MAGIC 
# MAGIC It is very difficult to combine processing of batch and real-time data as is evidenced by the diagram below.
# MAGIC 
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/lambda.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Updates in the Lakehouse
# MAGIC 
# MAGIC Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, etc.).
# MAGIC 
# MAGIC - **Silver** tables will provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
# MAGIC 
# MAGIC - **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. 
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data.
# MAGIC 
# MAGIC Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified (and artificially generated) medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Setup
# MAGIC 
# MAGIC The following notebook declares a number of path variables that will be used throughout this demo.

# COMMAND ----------

# MAGIC %run "./Includes/multi-hop-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Simulator
# MAGIC Spark Structured Streaming can automatically process files as they land in your cloud object stores. To simulate this process, you will be asked to run the following operation several times throughout the course.

# COMMAND ----------

NewFile.arrival()
display(dbutils.fs.ls(streamingPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table: Ingesting Raw JSON Recordings
# MAGIC 
# MAGIC Note that we'll be keeping our data in its raw format during this stage by reading our JSON as a text file. In this way, we ensure that all data will make it into our bronze Delta table. If any of our records are corrupted or have different schema, we can build downstream logic to decide how to handle these exceptions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream
# MAGIC Note that while you need to use the Spark DataFrame API to set up a streaming read, once configured you can immediately register a temp view to leverage Spark SQL for streaming transformations on your data.

# COMMAND ----------

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text")
  .schema("data STRING")
  .option("maxFilesPerTrigger", 1)  # This is used for testing to simulate 1 file arriving at a time.  Generally, don't set this in production.
  .load(streamingPath)
  .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Encoding the receipt time and the name of our dataset would allow us to use a single bronze table for multiple different data sources. This multiplex table design replicates the semi-structured nature of data stored in most data lakes while guaranteeing ACID transactions.
# MAGIC 
# MAGIC Downstream, we'll be able to subscribe to this table using the `dataset` field as a predicate, giving us a single table with read-after-write consistency guarantees as a source for multiple different queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT current_timestamp() receipt_time, "recordings" dataset, *
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream using Delta Lake
# MAGIC 
# MAGIC #### General Notation
# MAGIC Use this format to write a streaming job to a Delta Lake table.
# MAGIC 
# MAGIC ```
# MAGIC (myDF
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .option("checkpointLocation", checkpointPath)
# MAGIC   .outputMode("append")
# MAGIC   .start(path)
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC #### Output Modes
# MAGIC Notice, besides the "obvious" parameters, specify `outputMode`, which can take on these values
# MAGIC * `append`: add only new records to output sink
# MAGIC * `complete`: rewrite full output - applicable to aggregations operations
# MAGIC 
# MAGIC #### Checkpointing
# MAGIC 
# MAGIC - When defining a Delta Lake streaming query, one of the options that you need to specify is the location of a checkpoint directory.
# MAGIC `.option("checkpointLocation", "/path/to/checkpoint/directory/")`
# MAGIC - This is actually a structured streaming feature. It stores the current state of your streaming job. Should your streaming job stop for some reason and you restart it, it will continue from where it left off.
# MAGIC - If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC - Also note that every streaming job should have its own checkpoint directory: no sharing.

# COMMAND ----------

(spark.table("recordings_bronze_temp")
  .writeStream
  .format("delta")
  .option("checkpointLocation", bronzeCheckpoint)
  .outputMode("append")
  .start(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Trigger another file arrival with the following cell and you'll see the changes immediately detected by the streaming query you've written.

# COMMAND ----------

# Display how many records are in our table so we can watch it grow.
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{bronzePath}`"))

# COMMAND ----------

NewFile.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Parsed Recording Data
# MAGIC 
# MAGIC The first of our silver tables will subscribe to the `recordings` dataset in the multiplex table and parse the JSON payload. The logic here is intended to just parse our JSON payload, which will enforce that this data matches the defined schema and validate the data quality of the recordings data.

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(bronzePath)
  .createOrReplaceTempView("bronze_unparsed_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW recordings_parsed_temp AS
# MAGIC   SELECT json.device_id device_id, json.mrn mrn, json.heartrate heartrate, json.time time 
# MAGIC   FROM (
# MAGIC     SELECT from_json(data, "device_id INTEGER, mrn LONG, heartrate DOUBLE, time DOUBLE") json
# MAGIC     FROM bronze_unparsed_temp
# MAGIC     WHERE dataset = "recordings")

# COMMAND ----------

(spark.table("recordings_parsed_temp")
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", recordingsParsedCheckpoint)
  .start(recordingsParsedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC As new files arrived and are parsed into the upstream table, this query will automatically pick up those changes.

# COMMAND ----------

# Display how many records are in our table so we can watch it grow.
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{recordingsParsedPath}`"))

# COMMAND ----------

NewFile.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Static Personally Identifable Information (PII) Lookup Table
# MAGIC The ACID guarantees that Delta Lake brings to your data are managed at the table level, enforced as transactions complete and data is committed to storage. If you choose to merge these data with other data sources, be aware of how those sources version data and what sort of consistency guarantees they have.
# MAGIC 
# MAGIC In this simplified demo, we are loading a static CSV file to add patient data to our recordings. In production, we could use Databricks' [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) feature to keep an up-to-date view of these data in our Delta Lake.

# COMMAND ----------

(spark
  .read
  .format("csv")
  .schema("mrn STRING, name STRING")
  .option("header", True)
  .load(f"{source_dir}/patient/patient_info.csv")
  .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Enriched Recording Data
# MAGIC As a second hop in our silver level, we will do the follow enrichments and checks:
# MAGIC - Our recordings data will be joined with the PII to add patient names
# MAGIC - The time for our recordings will be parsed to the format `'yyyy-MM-dd HH:mm:ss'` to be human-readable
# MAGIC - We will exclude heart rates that are <= 0, as we know that these either represent the absence of the patient or an error in transmission

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(recordingsParsedPath)
  .createOrReplaceTempView("silver_recordings_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM silver_recordings_temp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
  .writeStream
  .format("delta")
  .option("checkpointLocation", recordingsEnrichedCheckpoint)
  .outputMode("append")
  .start(recordingsEnrichedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Trigger another new file and wait for it propagate through both previous queries.

# COMMAND ----------

# Display how many records are in our table so we can watch it grow.
display(spark.sql(f"SELECT COUNT(*) FROM delta.`{recordingsEnrichedPath}`"))

# COMMAND ----------

NewFile.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table: Daily Averages
# MAGIC 
# MAGIC Here we read a stream of data from `recordingsEnrichedPath` and write another stream to create an aggregate gold table of daily averages for each patient.

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(recordingsEnrichedPath)
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, MEAN(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we're using `.trigger(once=True)` below. This provides us the ability to continue to use the strengths of structured streaming while trigger this job as a single batch. To recap, these strengths include:
# MAGIC - exactly once end-to-end fault tolerant processing
# MAGIC - automatic detection of changes in upstream data sources
# MAGIC 
# MAGIC If we know the approximate rate at which our data grows, we can appropriately size the cluster we schedule for this job to ensure fast, cost-effective processing. The customer will be able to evaluate how much updating this final aggregate view of their data costs and make informed decisions about how frequently this operation needs to be run.
# MAGIC 
# MAGIC Downstream processes subscribing to this table do not need to re-run any expensive aggregations. Rather, files just need to be de-serialized and then queries based on included fields can quickly be pushed down against this already-aggregated source.

# COMMAND ----------

(spark.table("patient_avg")
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", dailyAvgCheckpoint)
  .trigger(once=True)
  .start(dailyAvgPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Daily Patient Averages Table to the Hive Metastore
# MAGIC 
# MAGIC We'll create an unmanaged table called `daily_patient_avg` using `DELTA`. This provides our BI analysts and data scientists easy access to these data.

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS daily_patient_avg
""")
spark.sql(f"""
  CREATE TABLE daily_patient_avg
  USING DELTA
  LOCATION '{dailyAvgPath}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Important Considerations for `complete` Output with Delta
# MAGIC 
# MAGIC When using `complete` output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
# MAGIC 
# MAGIC **NOTE**: Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to [Delta Streaming: Ignoring Updates and Deletes](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes).
# MAGIC 
# MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC Note the above table includes all days for all users. If the predicates for our ad hoc queries match the data encoded here, we can push down our predicates to files at the source and very quickly generate more limited aggregate views.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Remaining Records
# MAGIC The following cell will land additional files for the rest of 2020 in your source directory. You'll be able to see these process through the first 3 tables in your Delta Lake, but will need to re-run your final query to update your `daily_patient_avg` table, since this query uses the trigger once syntax.

# COMMAND ----------

NewFile.arrival(continuous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Finally, make sure all streams are stopped.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Delta Lake and Structured Streaming combine to provide near real-time analytic access to data in the lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html#as-a-sink" target="_blank">Delta Streaming Write Notation</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tagatha Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="http://lambda-architecture.net/#" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Reading structured streams from Kafka</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
# MAGIC * <a href="https://docs.databricks.com/delta/delta-intro.html#case-study-multi-hop-pipelines#" target="_blank">Multi Hop Pipelines</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
