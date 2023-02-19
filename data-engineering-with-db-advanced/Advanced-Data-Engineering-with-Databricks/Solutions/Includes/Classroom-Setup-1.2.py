# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="1.2"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def create_raw_data():
    import time
    from pyspark.sql import functions as F

    start = int(time.time())
    print(f"Creating raw_data", end="...")
    
    (spark.read
          .load(f"{DA.hidden.datasets}/bronze")
          .select(F.col("key").cast("string").alias("key"), "value", "topic", "partition", "offset", "timestamp")
          .filter("week_part > '2019-49'")
          .write
          .format("parquet")
          .option("path", f"{DA.paths.working_dir}/raw_parquet")
          .saveAsTable("raw_data"))

    # DA.raw_data_tbl = "raw_data" 
    # spark.conf.set("da.raw_data_tbl", DA.raw_data_tbl)
    # spark.read.format("parquet").load(f"{DA.paths.working_dir}/raw_parquet").createOrReplaceTempView("raw_data")

    total = spark.read.table("raw_data").count()
    assert total == 6443331, f"Expected 6,443,331 records, found {total:,} in raw_data"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    

# COMMAND ----------

DA.cleanup()
DA.init()

create_raw_data()

DA.conclude_setup()

