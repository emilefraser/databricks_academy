# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream-Static Joins
# MAGIC 
# MAGIC In this lesson, you'll join streaming heart rate data with the completed workouts table.
# MAGIC 
# MAGIC We'll be creating the table **`workout_bpm`** in our architectural diagram.
# MAGIC 
# MAGIC This pattern will take advantage of Delta Lake's ability to guarantee that the latest version of a table is returned each time it is queried.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_workout_bpm.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe guarantees around versioning and matching for stream-static joins
# MAGIC - Leverage Spark SQL and PySpark to process stream-static joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC **NOTE**: The setup script includes logic to define a **`user_lookup`** table required for the join below.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.5

# COMMAND ----------

# MAGIC %md
# MAGIC Set up your streaming temp view. Note that we will only be streaming from **one** of our tables. The **`completed_workouts`** table is no longer streamable as it breaks the requirement of an ever-appending source for Structured Streaming. However, when performing a stream-static join with a Delta table, each batch will confirm that the newest version of the static Delta table is being used.

# COMMAND ----------

(spark.readStream
      .table("heart_rate_silver")
      .createOrReplaceTempView("TEMP_heart_rate_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Stream-Static Join to Align Workouts to Heart Rate Recordings
# MAGIC 
# MAGIC Below we'll configure our query to join our stream to our **`completed_workouts`** table.
# MAGIC 
# MAGIC Note that our heart rate recordings only have **`device_id`**, while our workouts use **`user_id`** as the unique identifier. We'll need to use our **`user_lookup`** table to match these values. Because all tables are Delta Lake tables, we're guaranteed to get the latest version of each table during each microbatch transaction.
# MAGIC 
# MAGIC Importantly, our devices occasionally send messages with negative recordings, which represent a potential error in the recorded values. We'll need to define predicate conditions to ensure that only positive recordings are processed. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_workout_bpm AS
# MAGIC   SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
# MAGIC   FROM TEMP_heart_rate_silver c
# MAGIC   INNER JOIN (
# MAGIC     SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
# MAGIC     FROM completed_workouts a
# MAGIC     INNER JOIN user_lookup b
# MAGIC     ON a.user_id = b.user_id) d
# MAGIC   ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
# MAGIC   WHERE c.bpm_check = 'OK'

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the streaming portion of the join drives this join process. As currently implemented, this means that records from the **`heart_rate_silver`** table will only appear in our results table if a matching record has been written to the **`completed_workouts`** table prior to processing this query.
# MAGIC 
# MAGIC Stream-static joins are not stateful, meaning that we cannot configure our query to wait for records to appear in the right side of the join prior to calculating the results. When leveraging stream-static joins, make sure to be aware of potential limitations for unmatched records. (Note that a separate batch job could be configured to find and insert records that were missed during incremental execution).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream in Append Mode
# MAGIC 
# MAGIC Below, we'll use our streaming temp view from above to insert new values into our **`workout_bpm`** table.

# COMMAND ----------

def process_workout_bpm():
    query = (spark.table("TEMP_workout_bpm")
                  .writeStream
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm")
                  .trigger(availableNow=True)
                  .table("workout_bpm"))
    
    query.awaitTermination()
    
process_workout_bpm()

# COMMAND ----------

# MAGIC %md
# MAGIC Explore this results table below.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM workout_bpm

# COMMAND ----------

# MAGIC %md
# MAGIC If desired, process another batch through all tables and update these results.

# COMMAND ----------

DA.daily_stream.load()          # Load one new day for DA.paths.source_daily
DA.process_bronze()             # Process through the bronze table
DA.process_heart_rate_silver()  # Process the heart_rate_silver table
DA.process_workouts_silver()    # Process the workouts_silver table
DA.process_completed_workouts() # Process the completed_workouts table

process_workout_bpm()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

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
