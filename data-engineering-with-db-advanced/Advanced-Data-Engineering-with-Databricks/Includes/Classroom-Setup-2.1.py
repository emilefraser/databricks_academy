# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"

DA.cleanup()
DA.init()
DA.conclude_setup()

