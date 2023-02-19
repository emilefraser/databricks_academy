# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Scheduling a Batch Job
# MAGIC 
# MAGIC This notebook is designed to be scheduled as a batch job. 
# MAGIC 
# MAGIC As a general rule, before scheduling notebooks, make sure you comment out:
# MAGIC - Any file removal commands added during development
# MAGIC - Any commands dropping or creating databases or tables (unless you wish these to be created fresh with each execution)
# MAGIC - Any arbitrary actions/SQL queries that materialize results to the notebook (unless a human will regularly review this visual output)
# MAGIC 
# MAGIC ### Scheduling Against an Interactive Cluster
# MAGIC 
# MAGIC Because our data is small and the query we run here will complete fairly quickly, we'll take advantage of our already-on compute while scheduling this notebook.
# MAGIC 
# MAGIC You may choose to manually trigger this job, or set it to a schedule to update each minute.
# MAGIC 
# MAGIC You can click **`Run Now`** if desired, or just wait until the top of the next minute for this to trigger automatically.
# MAGIC 
# MAGIC ### Best Practice: Warm Pools
# MAGIC 
# MAGIC During this demo, we're making the conscious choice to take advantage of already-on compute to reduce friction and complexity for getting our code running. In production, jobs like this one (short duration and triggered frequently) should be scheduled against <a href="https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/" target="_blank">warm pools</a>.
# MAGIC 
# MAGIC Pools provide you the flexibility of having compute resources ready for scheduling jobs against while removing DBU charges for idle compute. DBUs billed are for jobs rather than all-purpose workloads (which is a lower cost). Additionally, using pools instead of interactive clusters eliminates the potential for resource contention between jobs sharing a single cluster or between scheduled jobs and interactive queries.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.3

# COMMAND ----------

from pyspark.sql import functions as F

# user_bins
def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

lookupDF = spark.table("user_lookup").select("alt_id", "user_id")
binsDF = spark.table("users").join(lookupDF, ["alt_id"], "left").select("user_id", age_bins(F.col("dob")),"gender", "city", "state")

(binsDF.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable("user_bins"))

# COMMAND ----------

# completed_workouts
spark.sql("""
    CREATE OR REPLACE TEMP VIEW TEMP_completed_workouts AS (
      SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
      FROM (
        SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
        FROM workouts_silver
        WHERE action = "start") a
      LEFT JOIN (
        SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
        FROM workouts_silver
        WHERE action = "stop") b
      ON a.user_id = b.user_id AND a.session_id = b.session_id
    )
""")

(spark.table("TEMP_completed_workouts").write
      .mode("overwrite")
      .saveAsTable("completed_workouts"))

# COMMAND ----------

#workout_bpm
spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")

spark.sql("""
  SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
  FROM TEMP_heart_rate_silver c
  INNER JOIN (
    SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
    FROM completed_workouts a
    INNER JOIN user_lookup b
    ON a.user_id = b.user_id) d
  ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
  WHERE c.bpm_check = 'OK'""").createOrReplaceTempView("TEMP_workout_bpm")

query = (spark.table("TEMP_workout_bpm")
              .writeStream
              .outputMode("append")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm")
              .trigger(availableNow=True)
              .table("workout_bpm"))

query.awaitTermination()

# COMMAND ----------

# workout_bpm_summary
spark.readStream.table("workout_bpm").createOrReplaceTempView("TEMP_workout_bpm")

df = (spark.sql("""
SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
FROM user_bins a
INNER JOIN
  (SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
  FROM TEMP_workout_bpm
  GROUP BY user_id, workout_id, session_id) b
ON a.user_id = b.user_id"""))

query = (df.writeStream
           .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm_summary")
           .outputMode("complete")
           .trigger(availableNow=True)
           .table("workout_bpm_summary"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Unlike other lessons, we will **NOT** be be executing our **`DA.cleanup()`** command<br/>
# MAGIC as we want these assets to persist through all the notebooks in this demo.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
