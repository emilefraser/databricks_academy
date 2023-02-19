# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Stored Views
# MAGIC 
# MAGIC In this notebook, we'll give a quick overview of how stored views are created and managed. Recall that stored views differ from DataFrames and temp views by persisting to a database (allowing other users to leverage pre-defined logic to materialize results). Views register the logic required to calculate a result (which will be evaluated when the query is executed). Views defined against Delta Lake sources are guaranteed to always query the latest version of each data source.
# MAGIC 
# MAGIC The goal of this notebook is to generate a view that allows the analysts from our partner gyms to examine how use of Moovio devices and workouts impact gym activity.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_gym_report.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Display the query plan associated with a view
# MAGIC - Describe how results are materialized from Delta Lake tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Gym Logs
# MAGIC 
# MAGIC Start by reviewing the schema for your gym logs.

# COMMAND ----------

gymDF = spark.table("gym_mac_logs")
gymDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC A Spark DataFrame and a view are nearly identical constructs. By calling **`explain`** on our DataFrame, we can see that our source table is a set of instructions to deserialize the files containing our data.

# COMMAND ----------

gymDF.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Workout Data
# MAGIC 
# MAGIC Rather than trying to capture every possible metric in our view, we'll create a summary of values that might be of interest to our gym analysts.
# MAGIC 
# MAGIC The data we're receiving from our gyms indicates the first and last timestamp recorded for user devices, indicated by mac address.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gym_mac_logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construct a Query
# MAGIC 
# MAGIC Our **`completed_workouts`** table indicates start and stop time for each user workout.
# MAGIC 
# MAGIC Use the cell below to construct a query that identifies:
# MAGIC - Each date a user completed at least one workout
# MAGIC - The earliest **`start_time`** for any workout each day
# MAGIC - The latest **`end_time`** for any workout each day
# MAGIC - The list of all workouts completed by a user each day

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC SELECT * 
# MAGIC FROM completed_workouts
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expand on the Query
# MAGIC 
# MAGIC Now we'll join this data back to the MAC logs sent by the gym to create our view.
# MAGIC 
# MAGIC We'll retain the **`mac_address`** as our identifier, which we can grab from the **`user_lookup`** table.
# MAGIC 
# MAGIC We'll also add columns to calculate the total number of minutes elapsed during a user's visit to the gym, as well as the total number of minutes elapsed between the beginning of their first workout and the end of their final workout.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gym, mac_address, date, workouts, (last_timestamp - first_timestamp)/60 minutes_in_gym, (to_unix_timestamp(end_workout) - to_unix_timestamp(start_workout))/60 minutes_exercising
# MAGIC FROM gym_mac_logs c
# MAGIC INNER JOIN (
# MAGIC   SELECT b.mac_address, to_date(start_time) date, collect_set(workout_id) workouts, min(start_time) start_workout, max(end_time) end_workout
# MAGIC       FROM completed_workouts a
# MAGIC       INNER JOIN user_lookup b
# MAGIC       ON a.user_id = b.user_id
# MAGIC       GROUP BY mac_address, to_date(start_time)
# MAGIC   ) d
# MAGIC   ON c.mac = d.mac_address AND to_date(CAST(c.first_timestamp AS timestamp)) = d.date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register View with Final Logic
# MAGIC 
# MAGIC Create a (non-temporary) view called **`gym_user_stats`** using the query above.
# MAGIC 
# MAGIC **`CREATE VIEW IF NOT EXISTS gym_user_stats AS (...)`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE YOUR VIEW HERE

# COMMAND ----------

# Check your work
assert spark.sql("SHOW TABLES").filter("tableName='gym_user_stats'").count() >= 1, "View 'gym_user_stats' does not exist."
assert spark.sql("SHOW TABLES").filter("tableName='gym_user_stats'").first()["isTemporary"]==False, "View 'gym_user_stats' should be not temporary."
assert spark.sql("DESCRIBE EXTENDED gym_user_stats").filter("col_name='Type'").first()['data_type']=='VIEW', "Found a table 'gym_user_stats' when a view was expected."
assert spark.table("gym_user_stats").count() == 304, "Incorrect query used for view 'gym_user_stats'."
print("All tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our view is simply storing the Spark plan for our query.

# COMMAND ----------

spark.table("gym_user_stats").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC When we execute a query against this view, we will process the plan to generate the logically correct result.
# MAGIC 
# MAGIC Note that while the data may end up in the Delta Cache, this result is not guaranteed to be persisted, and is only cached for the currently active cluster.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gym_user_stats
# MAGIC WHERE gym = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## An Aside on ACLs
# MAGIC 
# MAGIC While Databricks has extensive support for ACLs, by default these are not enforced for standard data engineering clusters. As such, the default permissions for this view are set to all users, and no owner has been declared.

# COMMAND ----------

import py4j

try:
    spark.sql("SHOW GRANT ON VIEW gym_user_stats")
    
except py4j.protocol.Py4JJavaError as e:
    print("Error: " + e.java_exception.getMessage())
    print("Solution: Consider enabling Table Access Control to demonstrate this feature.")

# COMMAND ----------

# MAGIC %md
# MAGIC While the privileges for this view may not be especially sensitive, we can see that our bronze table (which contains ALL our raw data) is also currently stored in this fashion.
# MAGIC 
# MAGIC Again, ACLs are primarily intended for managing data access within the Databricks workspace for BI and data science use cases. For sensitive data engineering data, you will want to make sure that you limit access to the storage containers using your cloud identity management.

# COMMAND ----------

try:
    spark.sql("SHOW GRANT ON TABLE bronze")
    
except py4j.protocol.Py4JJavaError as e:
    print("Error: " + e.java_exception.getMessage())
    print("Solution: Consider enabling Table Access Control to demonstrate this feature.")

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
