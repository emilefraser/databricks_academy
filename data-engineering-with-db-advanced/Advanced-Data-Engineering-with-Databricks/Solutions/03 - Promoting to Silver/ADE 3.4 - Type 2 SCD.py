# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Type 2 Slowly Changing Data
# MAGIC 
# MAGIC In this notebook, we'll create a silver table that contains the information we'll need to link workouts back to our heart rate recordings.
# MAGIC 
# MAGIC We'll use a Type 2 table to record this data, encoding the start and end times for each session. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_completed_workouts.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe how Slowly Changing Dimension tables can be implemented in the Lakehouse
# MAGIC - Use custom logic to implement a SCD Type 2 table with batch overwrite logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Set up path and checkpoint variables (these will be used later).

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review workouts_silver Table
# MAGIC Several helper functions was defined to land and propagate a batch of data to the **`workouts_silver`** table.
# MAGIC 
# MAGIC This table is created by 
# MAGIC * Starting a stream against the **`bronze`** table
# MAGIC * Filtering all records by **`topic = 'workout'`**
# MAGIC * Deduping the data 
# MAGIC * Merging non-matching records into **`owrkouts_silver`**
# MAGIC 
# MAGIC ...roughly the same strategy we used earlier to create the **`heart_rate_silver`** table

# COMMAND ----------

DA.daily_stream.load()       # Load another day's data
DA.process_bronze()          # Update the bronze table
DA.process_workouts_silver() # Update the workouts_silver table

# COMMAND ----------

# MAGIC %md
# MAGIC Review the **`workouts_silver`** data.

# COMMAND ----------

workout_df = spark.read.table("workouts_silver")
display(workout_df)

# COMMAND ----------

# MAGIC %md
# MAGIC For this data, the **`user_id`** and **`session_id`** form a composite key. 
# MAGIC 
# MAGIC Each pair should eventually have 2 records present, marking the "start" and "stop" action for each workout.

# COMMAND ----------

aggregate_df = workout_df.groupby("user_id", "session_id").count()
display(aggregate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we'll be triggering a shuffle in this notebook, we'll be explicit about how many partitions we want at the end of our shuffle.
# MAGIC 
# MAGIC As before, we can use the current level of parallelism (max number of cores) as our upper bound for shuffle partitions.

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Completed Workouts Table
# MAGIC 
# MAGIC The query below matches our start and stop actions, capturing the time for each action. The **`in_progress`** field indicates whether or not a given workout session is ongoing.

# COMMAND ----------

def process_completed_workouts():
    spark.sql(f"""
        CREATE OR REPLACE TABLE completed_workouts 
        AS (
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
    
process_completed_workouts()

# COMMAND ----------

# MAGIC %md
# MAGIC You can now perform a query directly on your **`completed_workouts`** table to check your results. 

# COMMAND ----------

total = spark.table("completed_workouts").count() # .sql("SELECT COUNT(*) FROM completed_workouts") 
print(f"{total:3} total")

total = spark.table("completed_workouts").filter("in_progress=true").count()
print(f"{total:3} where record is still awaiting end time")

total = spark.table("completed_workouts").filter("end_time IS NOT NULL").count()
print(f"{total:3} where end time has been recorded")

total = spark.table("completed_workouts").filter("start_time IS NOT NULL").count()
print(f"{total:3} where end time arrived after start time")

total = spark.table("completed_workouts").filter("in_progress=true AND end_time IS NULL").count()
print(f"{total:3} where they are in_progress AND have an end_time")

# COMMAND ----------

# MAGIC %md
# MAGIC Use the functions below to propagate another batch of records through the pipeline to this point.

# COMMAND ----------

DA.daily_stream.load()       # Load another day's data
DA.process_bronze()          # Update the bronze table
DA.process_workouts_silver() # Update the workouts_silver table

process_completed_workouts() # Update the completed_workouts table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC AS total 
# MAGIC FROM completed_workouts

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
