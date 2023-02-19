# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="8.4"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

# Do not cleanup!
# Intentionally keeping State from the previous notebook
# DA.cleanup() 
DA.init()

DA.paths.streaming_logs_json = f"{DA.paths.working_dir}/streaming_logs"
DA.paths.streaming_logs_delta = f"{DA.paths.working_dir}/streaming_logs_delta"

DA.conclude_setup()

