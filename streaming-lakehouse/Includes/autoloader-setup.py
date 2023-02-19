# Databricks notebook source

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

import pyspark.sql.functions as F
import re

course = "autoloader"

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "cleanup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

rawDataSource = f"{userhome}/source"
bronzeTable = f"{userhome}/bronze"
bronzeCheckpoint = f"{userhome}/bronze_checkpoint"
bronzeTableName = "bronze_event"

dbutils.fs.mkdirs(rawDataSource)

# COMMAND ----------

import time
import numpy as np
import pandas as pd
import json

class FileArrival:
    def __init__(self, rawDataSource):
        self.source = rawDataSource.replace("dbfs:", "/dbfs")
        self.deviceTypes = ["SensorTypeA", "SensorTypeB", "SensorTypeC", "SensorTypeD"]
        self.fileID = 0
    
    def prep(self, numFiles=10, numRows=100):
        for i in range(numFiles):
            self.newData()
            
    def newData(self, numRows=100, new_field=False, bad_timestamp=False):
            file = f"{self.source}/file-{self.fileID}.json"
            if bad_timestamp:
                startTime=time.time()
                timestamp = startTime + (self.fileID * 60) + np.random.randint(-10, 10, size=numRows)
            else:
                startTime=int(time.time()*1000)
                timestamp = startTime + (self.fileID * 60000) + np.random.randint(-10000, 10000, size=numRows)
            deviceId = np.random.randint(0, 100, size=numRows)
            deviceType = np.random.choice(self.deviceTypes, size=numRows)
            signalStrength = np.random.random(size=numRows)
            data = [timestamp, deviceId, deviceType, signalStrength]
            columns = ["timestamp", "deviceId", "deviceType", "signalStrength"]
            if new_field:
                metadata = [{"version":"1.2.3", "response":"200"}] * numRows
                data.append(metadata)
                columns.append("metadata")
            pd.DataFrame(data=zip(*data), 
                 columns = columns
                ).to_json(file, orient="records")
            self.fileID+=1

# COMMAND ----------

File = FileArrival(rawDataSource)
File.prep()

# COMMAND ----------

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

