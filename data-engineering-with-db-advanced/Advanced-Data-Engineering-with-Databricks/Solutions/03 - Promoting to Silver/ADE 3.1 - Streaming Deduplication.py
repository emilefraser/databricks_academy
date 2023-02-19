# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Deduplication
# MAGIC 
# MAGIC In this notebook, you'll learn how to eliminate duplicate records while working with Structured Streaming and Delta Lake. While Spark Structured Streaming provides exactly-once processing guarantees, many source systems will introduce duplicate records, which must be removed in order for joins and updates to produce logically correct results in downstream queries.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Apply **`dropDuplicates`** to streaming data
# MAGIC - Use watermarking to manage state information
# MAGIC - Write an insert-only merge to prevent inserting duplicate records into a Delta table
# MAGIC - Use **`foreachBatch`** to perform a streaming upsert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Declare a database and set all path variables.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Duplicate Records
# MAGIC 
# MAGIC Because Kafka provides at-least-once guarantees on data delivery, all Kafka consumers should be prepared to handle duplicate records.
# MAGIC 
# MAGIC The de-duplication methods shown here can also be applied when necessary in other parts of your Delta Lake applications.
# MAGIC 
# MAGIC Let's start by identifying the number of duplicate records in our **`bpm`** topic of the bronze table.

# COMMAND ----------

total = (spark.read
              .table("bronze")
              .filter("topic = 'bpm'")
              .count())

print(f"Total: {total:,}")

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

old_total = (spark.read
                  .table("bronze")
                  .filter("topic = 'bpm'")
                  .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                  .select("v.*")
                  .dropDuplicates(["device_id", "time"])
                  .count())

print(f"Old Total: {old_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC It appears that around 10-20% of our records are duplicates. Note that here we're choosing to apply deduplication at the silver rather than the bronze level. While we are storing some duplicate records, our bronze table retains a history of the true state of our streaming source, presenting all records as they arrived (with some additional metadata recorded). This allows us to recreate any state of our downstream system, if necessary, and prevents potential data loss due to overly aggressive quality enforcement at the initial ingestion as well as minimizing latencies for data ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read on the Bronze BPM Records
# MAGIC 
# MAGIC Here we'll bring back in our final logic from our last notebook.

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

bpm_df = (spark.readStream
               .table("bronze")
               .filter("topic = 'bpm'")
               .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
               .select("v.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC When dealing with streaming deduplication, there is a level of complexity compared to static data.
# MAGIC 
# MAGIC As each micro-batch is processed, we need to ensure:
# MAGIC - No duplicate records exist in the microbatch
# MAGIC - Records to be inserted are not already in the target table
# MAGIC 
# MAGIC Spark Structured Streaming can track state information for the unique keys to ensure that duplicate records do not exist within or between microbatches. Over time, this state information will scale to represent all history. Applying a watermark of appropriate duration allows us to only track state information for a window of time in which we reasonably expect records could be delayed. Here, we'll define that watermark as 30 seconds.
# MAGIC 
# MAGIC The cell below updates our previous query.

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'bpm'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("time", "30 seconds")
                   .dropDuplicates(["device_id", "time"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Only Merge
# MAGIC Delta Lake has optimized functionality for insert-only merges. This operation is ideal for de-duplication: define logic to match on unique keys, and only insert those records for keys that don't already exist.
# MAGIC 
# MAGIC Note that in this application, we proceed in this fashion because we know two records with the same matching keys represent the same information. If the later arriving records indicated a necessary change to an existing record, we would need to change our logic to include a **`WHEN MATCHED`** clause.
# MAGIC 
# MAGIC A merge into query is defined in SQL below against a view titled **`stream_updates`**.

# COMMAND ----------

sql_query = """
  MERGE INTO heart_rate_silver a
  USING stream_updates b
  ON a.device_id=b.device_id AND a.time=b.time
  WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Microbatch Function for **`foreachBatch`**
# MAGIC 
# MAGIC The Spark Structured Streaming **`foreachBatch`** method allows users to define custom logic when writing.
# MAGIC 
# MAGIC The logic applied during **`foreachBatch`** addresses the present microbatch as if it were a batch (rather than streaming) data.
# MAGIC 
# MAGIC The class defined in the following cell defines simple logic that will allow us to register any SQL **`MERGE INTO`** query for use in a Structured Streaming write.

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we're using SQL to write to our Delta table, we'll need to make sure this table exists before we begin.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS heart_rate_silver 
# MAGIC (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.user_db}/heart_rate_silver'

# COMMAND ----------

# MAGIC %md
# MAGIC Now pass the previously defined **`sql_query`** to the **`Upsert`** class.

# COMMAND ----------

streaming_merge = Upsert(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC And then use this class in our **`foreachBatch`** logic.

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(streaming_merge.upsert_to_delta)
                   .outputMode("update")
                   .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our number of unique entries that have been processed to the **`heart_rate_silver`** table matches our batch de-duplication query from above.

# COMMAND ----------

new_total = spark.read.table("heart_rate_silver").count()

print(f"Old Total: {old_total:,}")
print(f"New Total: {new_total:,}")

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
