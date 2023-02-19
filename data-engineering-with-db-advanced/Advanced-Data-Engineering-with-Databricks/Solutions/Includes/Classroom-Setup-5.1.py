# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="5.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

DA.create_bronze_table()          # Clone (don't processes) the bronze table
DA.create_user_lookup()           # Create the user-lookup table
DA.create_gym_mac_logs()          # Create the gym_mac_logs()
print()

DA.process_workouts_silver()      # Process the workouts_silver table
DA.process_completed_workouts()   # Process the completed_workouts table

DA.conclude_setup()

