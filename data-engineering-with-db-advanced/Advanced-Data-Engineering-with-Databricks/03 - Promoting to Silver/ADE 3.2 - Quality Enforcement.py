# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Enforcement
# MAGIC 
# MAGIC One of the main motivations for using Delta Lake to store data is that you can provide guarantees on the quality of your data. While schema enforcement is automatic, additional quality checks can be helpful to ensure that only data that meets your expectations makes it into your Lakehouse.
# MAGIC 
# MAGIC This notebook will review a few approaches to quality enforcement. Some of these are Databricks-specific features, while others are general design principles.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Add check constraints to Delta tables
# MAGIC - Describe and implement a quarantine table
# MAGIC - Apply logic to add data quality tags to Delta tables

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Constraints
# MAGIC 
# MAGIC Databricks allows <a href="https://docs.databricks.com/delta/delta-constraints.html" target="_blank">table constraints</a> to be set on Delta tables.
# MAGIC 
# MAGIC Table constraints apply boolean filters to columns within a table and prevent data that does not fulfill these constraints from being written.

# COMMAND ----------

# MAGIC %md
# MAGIC Start by looking at our existing tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC If these exist, table constraints will be listed under the **`properties`** of the extended table description.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC When defining a constraint, be sure to give it a human-readable name. (Note that names are not case sensitive.)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE heart_rate_silver ADD CONSTRAINT date_within_range CHECK (time > '2017-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC None of the existing data in our table violated this constraint. Both the name and the actual check are displayed in the **`properties`** field.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC But what happens if the conditions of the constraint aren't met?
# MAGIC 
# MAGIC We know that some of our devices occasionally send negative **`bpm`** recordings.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM heart_rate_silver
# MAGIC WHERE heartrate <= 0 

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake will prevent us from applying a constraint that existing records violate.

# COMMAND ----------

import pyspark
try:
    spark.sql("ALTER TABLE heart_rate_silver ADD CONSTRAINT validbpm CHECK (heartrate > 0);")
    raise Exception("Expected failure")

except pyspark.sql.utils.AnalysisException as e:
    print("Failed as expected...")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice below how we failed to applied the constraint

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC How do we deal with this? 
# MAGIC 
# MAGIC We could manually delete offending records and then set the check constraint, or set the check constraint before processing data from our bronze table.
# MAGIC 
# MAGIC However, if we set a check constraint and a batch of data contains records that violate it, the job will fail and we'll throw an error.
# MAGIC 
# MAGIC If our goal is to identify bad records but keep streaming jobs running, we'll need a different solution.
# MAGIC 
# MAGIC One idea would be to quarantine invalid records.
# MAGIC 
# MAGIC Note that if you need to remove a constraint from a table, the following code would be executed.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE heart_rate_silver DROP CONSTRAINT validbpm;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantining
# MAGIC 
# MAGIC The idea of quarantining is that bad records will be written to a separate location.
# MAGIC 
# MAGIC This allows good data to processed efficiently, while additional logic and/or manual review of erroneous records can be defined and executed away from the main pipeline.
# MAGIC 
# MAGIC Assuming that records can be successfully salvaged, they can be easily backfilled into the silver table they were deferred from.
# MAGIC 
# MAGIC Here, we'll implement quarantining by performing writes to two separate tables within a **`foreachBatch`** custom writer.

# COMMAND ----------

# MAGIC %md
# MAGIC Start by creating a table with the correct schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bpm_quarantine
# MAGIC     (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.user_db}/bpm_quarantine'

# COMMAND ----------

# MAGIC %md
# MAGIC With Structured Streaming operations, writing to an additional table can be accomplished within **`foreachBatch`** logic.
# MAGIC 
# MAGIC Below, we'll update the logic to add filters at the appropriate locations.
# MAGIC 
# MAGIC For simplicity, we won't check for duplicate records as we insert data into the quarantine table.

# COMMAND ----------

sql_query = """
MERGE INTO heart_rate_silver a
USING stream_updates b
ON a.device_id=b.device_id AND a.time=b.time
WHEN NOT MATCHED THEN INSERT *
"""

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, micro_batch_df, batch):
        micro_batch_df.filter("heartrate" > 0).createOrReplaceTempView(self.update_temp)
        micro_batch_df._jdf.sparkSession().sql(self.query)
        micro_batch_df.filter("heartrate" <= 0).write.format("delta").mode("append").saveAsTable("bpm_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that within the **`foreachBatch`** logic, the DataFrame operations are treating the data in each batch as if it's static rather than streaming.
# MAGIC 
# MAGIC As such, we use the **`write`** syntax instead of **`writeStream`**.
# MAGIC 
# MAGIC This also means that our exactly-once guarantees are relaxed. In our example above, we have two ACID transactions:
# MAGIC 1. Our SQL query executes to run an insert-only merge to avoid writing duplicate records to our silver table.
# MAGIC 2. We write a microbatch of records with negative heartrates to the **`bpm_quarantine`** table
# MAGIC 
# MAGIC If our job fails after our first transaction completes but before the second completes, we will re-execute the full microbatch logic on job restart.
# MAGIC 
# MAGIC However, because our insert-only merge already prevents duplicate records from being saved to our table, this will not result in any data corruption.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Flagging
# MAGIC To avoid multiple writes and managing multiple tables, you may choose to implement a flagging system to warn about violations while avoiding job failures.
# MAGIC 
# MAGIC Flagging is a low touch solution with little overhead.
# MAGIC 
# MAGIC These flags can easily be leveraged by filters in downstream queries to isolate bad data.
# MAGIC 
# MAGIC **`case`** / **`when`** logic makes this easy.
# MAGIC 
# MAGIC Run the following cell to see the compiled Spark SQL from the PySpark code below.

# COMMAND ----------

from pyspark.sql import functions as F

F.when(F.col("heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check")

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we'll just insert this logic as an additional transformation on a batch read of our bronze data to preview the output.

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

deduped_df = (spark.read
                  .table("bronze")
                  .filter("topic = 'bpm'")
                  .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                  .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM")
                                  .otherwise("OK")
                                  .alias("bpm_check"))
                  .dropDuplicates(["device_id", "time"]))

display(deduped_df)

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
