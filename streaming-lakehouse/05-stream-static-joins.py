# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Stream-Static Joins in the Lakehouse
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the behavior of Delta Lake when joining streaming and static tables
# MAGIC * Use Structured Streaming and Delta Lake to execute stateless joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Setup
# MAGIC 
# MAGIC The following notebook declares a number of path variables that will be used throughout this demo.

# COMMAND ----------

# MAGIC %run "./Includes/setup-stream-static"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified (and artificially generated) medical data loaded into two pre-defined Delta Lake tables.
# MAGIC 
# MAGIC #### `silver_recordings`
# MAGIC This table is an append-only validated granular view of facts; each row represents the heart rate of a patient.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### `pii`
# MAGIC This table stores only the most recent entry for each patient, and is written by upserting new records (Type 1 Slowly Changing Dimension table). Here, we simplify this data by only updating the patient's `weight` and the timestamp at which this data was entered.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |
# MAGIC | timestamp | long |
# MAGIC | weight | double |
# MAGIC 
# MAGIC Run the cells below to manually preview these tables. Note that only 1 week of data has been loaded.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_recordings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load DataFrames
# MAGIC 
# MAGIC Because our `silver_recordings` table is append-only, we can safely load this table as a streaming read.

# COMMAND ----------

silverDF = spark.readStream.table("silver_recordings")

# COMMAND ----------

# MAGIC %md
# MAGIC However, we know that `pii` will be regularly updated. This breaks the append-only requirement of Structured Streaming. We'll instead define a static read against this table.

# COMMAND ----------

piiDF = spark.table("pii")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Stream-Static Join
# MAGIC There's nothing especially complicated about the syntax used for a stream-static join. The primary thing to keep in mind is that our streaming table is driving the action. For each new batch of data we see arriving in `silver_recordings`, we'll process our join logic.

# COMMAND ----------

joinedDF = silverDF.join(piiDF, on=["mrn"])

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the `timestamp` field represents the updates to our `pii` table, while `time` tracks when our recordings were added. In this demo, we're processing a week of data in each batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Joined Data
# MAGIC We can now write our `joinedDF` as a regular stream. We'll at a `processingTime` trigger of 5 seconds.

# COMMAND ----------

(joinedDF.writeStream
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", enrichedCheckpoint)
    .toTable("enriched_recordings")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Simulator
# MAGIC For this notebook, the `Batch` object has been defined to process new data into both `silver_recordings` and `pii` tables. Expand the streaming monitor above and land a new batch.

# COMMAND ----------

Batch.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that, as expected, our join is being processed and written to the table `enriched_recordings`. Remember that our static read against `pii` was defined back when we only had 2 users in our table. Manually trigger a couple more batches using the cell above. Do you expect that new users will be automatically detected by our logic?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM enriched_recordings
# MAGIC ORDER BY time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC As we add more batches, we **do** see new patients and as well as updates to weights for existing users. Re-execute the SQL query in the previous cell to confirm this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Happens During a Stream-Static Join
# MAGIC 
# MAGIC On the streaming side, as expected, each time the streaming write triggers, a microbatch of data is processed representing all the new data that has arrived in the streaming source.
# MAGIC 
# MAGIC On the static side, behavior is the same as if we'd run a manual query against a Delta table (as above). For every batch processed, we reference the latest version of the Delta Lake table.
# MAGIC 
# MAGIC Note that this join is stateless; no watermark or windowing needs to be configured, and distinct keys from the join accumulate over time. Each streaming microbatch joins with the **most current version** of the static table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Streaming Dashboard
# MAGIC 
# MAGIC Run the cell below to generate a streaming display. Configure a line plot with `week` as the key, `name` as the grouping, and `weight` as the value.

# COMMAND ----------

spark.readStream.table("enriched_recordings").createOrReplaceTempView("enriched_recordings_TEMP")

display(spark.sql("""
    SELECT name, weekofyear(TIMESTAMP(time)) week, MAX(weight), AVG(heartrate)
    FROM enriched_recordings_TEMP
    GROUP BY name, weekofyear(TIMESTAMP(time))
    ORDER BY week DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below will continue to load new weeks of data one batch at a time. You should see patients enter and exit your plot, and their weights flucutating over time.

# COMMAND ----------

Batch.arrival(continuous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that because these joins are stateless, delayed data can lead to inconsistent results. In our example, if a patient's updated weight record had not been processed when new heart rate recordings came in, we would continue to join with the previous weight. Note that in our example, this inconsistency is minor and would be unlikely to significantly impact dashboards or analytics downstream. Indeed, our patient's weight is constantly fluctuating, and we are only occassionally taking an updated snapshot of it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Make sure all streams are stopped before exiting the notebook.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Delta Lake's ability to provide access to the newest version of tables using both streaming and static reads enables many unique patterns and use cases in the Lakehouse. Stream-static joins are a powerful tool that can enrich data in near real-time.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
