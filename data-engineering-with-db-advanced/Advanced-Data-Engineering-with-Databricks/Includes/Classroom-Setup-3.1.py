# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="3.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

# Create the user datasets
create_date_lookup()              # Create static copy of date_lookup
print()

init_source_daily()               # Create the data factory
DA.daily_stream.load()            # Load one new day for DA.paths.source_daily

# COMMAND ----------

DA.conclude_setup()

