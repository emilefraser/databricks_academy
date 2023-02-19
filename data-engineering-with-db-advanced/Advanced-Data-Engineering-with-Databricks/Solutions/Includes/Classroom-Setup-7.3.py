# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="7.3"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

DA.create_user_lookup()           # Create the user-lookup table
DA.create_partitioned_bronze_table()
print()

DA.process_users()

# I'm too lazy to refactor this out - JDP
spark.sql("DROP TABLE delete_requests") 
dbutils.fs.rm(f"{DA.paths.user_db}/delete_requests", True)

DA.conclude_setup()

