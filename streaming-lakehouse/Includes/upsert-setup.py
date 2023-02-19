# Databricks notebook source
# MAGIC 
# MAGIC %run ./common-setup $mode="reset" $course="upsert"

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

bronzePath = userhome + "/bronze"

class FileArrival:
    def __init__(self, path, reset=True, max_batch=3):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.path = path
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.path, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("delta")
                    .save(self.path))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("delta")
                .save(self.path))
            self.batch += 1

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

Bronze = FileArrival(bronzePath)

silverPath = userhome + "/silver"

spark.sql(f"""
    CREATE TABLE silver
    DEEP CLONE delta.`{URI}/pii/silver`
    LOCATION '{silverPath}'
""")

