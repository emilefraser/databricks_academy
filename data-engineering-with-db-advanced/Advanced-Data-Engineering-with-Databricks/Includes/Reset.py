# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="reset"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.init()

# COMMAND ----------

rows = spark.sql(f"show databases").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(DA.db_name_prefix):
        print(db_name)
        spark.sql(f"DROP DATABASE {db_name} CASCADE")

# COMMAND ----------

if DA.paths.exists(DA.working_dir_prefix):
    print(DA.working_dir_prefix)
    dbutils.fs.rm(DA.working_dir_prefix, True)

# COMMAND ----------

DA.install_datasets(reinstall=True)

