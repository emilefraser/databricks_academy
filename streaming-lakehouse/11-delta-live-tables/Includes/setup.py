# Databricks notebook source
import pyspark.sql.functions as F
import re

course = "dlt"

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

streamingPath = userhome + "/source"
storage_location = userhome + "/output"

class FileArrival:
  def __init__(self):
    self.source = "/mnt/training/healthcare/tracker/streaming/"
    self.userdir = streamingPath + "/"
    self.curr_mo = 1
    
  def arrival(self, continuous=False):
    if self.curr_mo > 12:
      print("Data source exhausted\n")
    elif continuous == True:
      while self.curr_mo <= 12:
        curr_file = f"{self.curr_mo:02}.json"
        dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
        self.curr_mo += 1
    else:
      curr_file = f"{str(self.curr_mo).zfill(2)}.json"
      dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
      self.curr_mo += 1
      
NewFile = FileArrival()
