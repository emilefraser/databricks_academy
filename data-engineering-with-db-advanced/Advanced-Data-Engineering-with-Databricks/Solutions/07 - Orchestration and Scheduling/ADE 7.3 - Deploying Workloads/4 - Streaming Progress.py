# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Monitoring Streaming Progress
# MAGIC 
# MAGIC This notebook is configured to incrementally load the JSON logs written by the custom [StreamingQueryListener]($../../Includes/StreamingQueryListener).
# MAGIC 
# MAGIC Note that executing this on a small cluster in conjunction with streaming and batch operations may lead to significant slowdown. 
# MAGIC 
# MAGIC Ideally, all jobs should be scheduled on isolated jobs clusters.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.4

# COMMAND ----------

# MAGIC %md
# MAGIC The code below uses Auto Loader to incrementally load log data to a Delta Lake table.

# COMMAND ----------

query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/streaming_logs")
              .load(DA.paths.streaming_logs_json)
              .writeStream
              .option("mergeSchema", True)
              .option("checkpointLocation", f"{DA.paths.checkpoints}/streaming_logs")
              .trigger(availableNow=True)
              .start(DA.paths.streaming_logs_delta))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Query streaming logs below.

# COMMAND ----------

display(spark.read.load(DA.paths.streaming_logs_delta))

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
