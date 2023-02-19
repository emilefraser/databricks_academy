# Databricks notebook source
# MAGIC 
# MAGIC %run ./data-source

# COMMAND ----------

import pyspark.sql.functions as F
import re

dbutils.widgets.text("course", "airbnb")
course = dbutils.widgets.get("course")

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""
prefix = re.sub("[^a-zA-Z0-9]", "_", username.split("@")[0])
model_name=f"{prefix}_airbnb_rf"

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "reset")
mode = dbutils.widgets.get("mode")

if mode == "reset":

    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
    
    dbutils.notebook.run("./Includes/reset-model", 60, {"model_name":model_name})

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

filepath = f"{userhome}/bronze_chicago"

spark.sql(f"""
  CREATE TABLE bronze_chicago
  DEEP CLONE delta.`{URI}/bronze_chicago`
  LOCATION '{filepath}'
""")

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, reset=True, max_batch=6):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.silver = demohome+ "/silver"
        self.batch = 0
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.silver, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("file_alias") == self.batch)
                    .drop("file_alias")
                    .write
                    .mode("append")
                    .format("delta")
                    .option("path", self.silver)
                    .saveAsTable("silver_chicago"))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("file_alias") == self.batch)
                .drop("file_alias")
                .write
                .mode("append")
                .format("delta")
                .option("path", self.silver)
                .saveAsTable("silver_chicago"))
            self.batch += 1

# COMMAND ----------

File = FileArrival(userhome)
File.arrival()

