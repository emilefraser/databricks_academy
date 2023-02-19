# Databricks notebook source
# Does any work to reset the environment prior to testing.
import time
try:
  dbutils.fs.unmount("/mnt/training")
  time.sleep(15) # for some reason things are moving too fast
except:
  pass # Don't care!
  
try:
  dbutils.fs.ls("/mnt/training")
  raise Exception("Mount shouldn't exist")
except:
  # This is good, mount has been removed
  print("/mnt/training confirmed to not exist")

# COMMAND ----------

# MAGIC %run ./Classroom-Setup
