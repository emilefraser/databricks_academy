# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="1.4"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.sensors_prod_tbl = "sensors_prod"

def create_sensors_prod():
    import time
    import numpy as np
    import pandas as pd

    start = int(time.time())
    print(f"Creating {DA.sensors_prod_tbl}", end="...")

    numFiles=3
    numRows=1000
    fileID = 0
    deviceTypes = ["A", "B", "C", "D"]

    spark.conf.set("spark.databricks.io.cache.enabled", "false")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DA.sensors_prod_tbl} (
            time LONG COMMENT 'event timestamp in ms since epoch', 
            device_id LONG COMMENT 'device IDs, integer only',  
            sensor_type STRING COMMENT 'sensor type identifier; single upper case letter', 
            signal_strength DOUBLE COMMENT 'decimal value between 0 and 1')
        USING DELTA LOCATION '{DA.paths.working_dir}/prod/sensors'
    """);

    for i in range(numFiles):
        startTime=int(time.time()*1000)
        timestamp = startTime + (fileID * 60000) + np.random.randint(-10000, 10000, size=numRows)
        deviceId = np.random.randint(0, 100, size=numRows)
        deviceType = np.random.choice(deviceTypes, size=numRows)
        signalStrength = np.random.random(size=numRows)
        data = [timestamp, deviceId, deviceType, signalStrength]

        columns = ["time", "device_id", "sensor_type", "signal_strength"]

        tempDF = spark.createDataFrame(pd.DataFrame(data=zip(*data), columns = columns))
        tempDF.write.format("delta").mode("append").saveAsTable(DA.sensors_prod_tbl)
        fileID+=1
    
    total = spark.read.table(DA.sensors_prod_tbl).count()
    print(f"({int(time.time())-start} seconds, {total:,} records)")

# COMMAND ----------

DA.cleanup()
DA.init()

create_sensors_prod()

DA.conclude_setup()

# COMMAND ----------

def _check_files(table_name):
    filepath = spark.sql(f"DESCRIBE EXTENDED {table_name}").filter("col_name == 'Location'").select("data_type").collect()[0][0]
    filelist = dbutils.fs.ls(filepath)
    filecount = len([file for file in filelist if file.name != "_delta_log/" ])
    print(f"Count of all data files in {table_name}: {filecount}\n")
    return filelist

DA.check_files = _check_files

