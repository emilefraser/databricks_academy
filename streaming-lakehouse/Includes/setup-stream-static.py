# Databricks notebook source
# MAGIC 
# MAGIC %run ./data-source

# COMMAND ----------

import pyspark.sql.functions as F
import re

dbutils.widgets.text("course", "joins")
course = dbutils.widgets.get("course")

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "reset")
mode = dbutils.widgets.get("mode")

if mode == "reset":

    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
    
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

spark.sql(f"CREATE TABLE enriched_recordings (device_id INTEGER, mrn LONG, heartrate DOUBLE, time DOUBLE, name STRING, timestamp LONG, weight DOUBLE) USING delta LOCATION '{userhome}/enriched_recordings'")

enrichedCheckpoint = userhome +"/checkpoint_enriched"

filepath = f"{userhome}/bronze_multiplex"

spark.sql(f"""
  CREATE TABLE bronze_multiplex
  DEEP CLONE delta.`{URI}/bronze_multiplex`
  LOCATION '{filepath}'
""")

# COMMAND ----------

import time

class FileArrival:
    def __init__(self, demohome, max_batch=53):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.silver = demohome + "/silver"
        self.pii = demohome + "/pii"
        self.batch = 2
        self.max_batch = max_batch
        
        spark.sql(f"CREATE TABLE silver_recordings (device_id INTEGER, mrn LONG, heartrate DOUBLE, time DOUBLE) USING delta LOCATION '{self.silver}'")
        spark.sql(f"CREATE TABLE pii (mrn LONG, name STRING, timestamp LONG, weight DOUBLE) USING delta LOCATION '{self.pii}'")

    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= self.max_batch:
                batchDF = self.rawDF.filter(F.col("week") == self.batch)
                batchDF.filter("topic == 'pii'").select(F.from_json("data", "mrn LONG, name STRING, timestamp LONG, weight DOUBLE").alias("data")).select("data.*").createOrReplaceTempView("new_pii")
                spark.sql("""
                    MERGE INTO pii a
                    USING new_pii b
                    ON a.mrn = b.mrn
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
                (batchDF.filter("topic == 'recordings'").select(F.from_json("data", "device_id INTEGER, mrn LONG, heartrate DOUBLE, time DOUBLE").alias("data")).select("data.*")
                    .write
                    .mode("append")
                    .format("delta")
                    .saveAsTable("silver_recordings"))
                time.sleep(2)
                self.batch += 1
        else:
            batchDF = self.rawDF.filter(F.col("week") == self.batch)
            batchDF.filter("topic == 'pii'").select(F.from_json("data", "mrn LONG, name STRING, timestamp LONG, weight DOUBLE").alias("data")).select("data.*").createOrReplaceTempView("new_pii")
            spark.sql("""
                MERGE INTO pii a
                USING new_pii b
                ON a.mrn = b.mrn
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)    
            (batchDF.filter("topic == 'recordings'").select(F.from_json("data", "device_id INTEGER, mrn LONG, heartrate DOUBLE, time DOUBLE").alias("data")).select("data.*")
                .write
                .mode("append")
                .format("delta")
                .saveAsTable("silver_recordings"))
            self.batch += 1

# COMMAND ----------

Batch = FileArrival(userhome)
Batch.arrival()

