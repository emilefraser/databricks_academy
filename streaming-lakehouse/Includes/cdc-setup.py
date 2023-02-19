# Databricks notebook source
# MAGIC 
# MAGIC %run ./common-setup $mode="reset" $course="cdc"

# COMMAND ----------

# MAGIC %run ./data-source

# COMMAND ----------

filepath = f"{userhome}/cdc_raw"

spark.sql(f"""
  CREATE TABLE cdc_raw
  DEEP CLONE delta.`{URI}/pii/raw`
  LOCATION '{filepath}'
""")

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, reset=True, max_batch=3):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.userdir = demohome+ "/raw"
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

Raw = FileArrival(userhome)

