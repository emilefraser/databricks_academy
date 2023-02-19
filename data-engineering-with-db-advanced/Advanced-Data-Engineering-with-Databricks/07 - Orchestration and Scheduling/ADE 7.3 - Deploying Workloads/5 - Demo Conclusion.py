# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion of Streaming Jobs Demo
# MAGIC 
# MAGIC Key Take Aways
# MAGIC * For high-utilization, we can combine many streams in one task.
# MAGIC * For highly-critical streams, we can isolate them to a single task.
# MAGIC * Difference between Always-On-Streams vs Run-Now-Streams & Available-Now-Streams
# MAGIC * Task-Specific Clusters vs General-Purpose Clusters
# MAGIC * Power of warm pools

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC * Stop/Pause the jobs
# MAGIC * Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
