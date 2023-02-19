# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="4.5"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

# Create the user datasets
create_date_lookup()              # Create static copy of date_lookup
DA.create_user_lookup()           # Create the user-lookup table
print()

init_source_daily()               # Create the data factory
# DA.daily_stream.load_through(6) # Load N days to DA.paths.source_daily
DA.daily_stream.load()            # Load one new day to DA.paths.source_daily

DA.process_bronze()               # Process through the bronze table
DA.process_heart_rate_silver()    # Process the heart_rate_silver table
DA.process_workouts_silver()      # Process the workouts_silver table
DA.process_completed_workouts()   # Process the completed_workouts table
DA.process_users()
DA.process_user_bins()

# COMMAND ----------

DA.conclude_setup()

