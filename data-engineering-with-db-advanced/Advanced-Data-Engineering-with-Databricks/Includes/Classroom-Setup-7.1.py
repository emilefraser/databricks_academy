# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="7.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.paths.cdc_stream = f"{DA.paths.working_dir}/streams/cdc"

class CdcStreamingFactory:
    def __init__(self, max_batch=3):
        self.batch = 1
        self.max_batch = max_batch
        dbutils.fs.mkdirs(DA.paths.cdc_stream)
        
    def load(self):
        import time
        from pyspark.sql import functions as F
        
        start = int(time.time())
        raw_df = spark.read.load(f"{DA.hidden.datasets}/pii/raw")
        
        if self.batch > self.max_batch:
            print("Data source exhausted")
            
        else:
            print(f"Loading batch #{self.batch} to the cdc stream", end="...")
            df = (raw_df.filter(F.col("batch") == self.batch)
                        .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated'))
            df.write.mode("append").format("json").save(DA.paths.cdc_stream)
            total = df.count()
            self.batch += 1
            print(f"({int(time.time())-start} seconds, {total:,} records)")


# COMMAND ----------

DA.cleanup()
DA.init()

DA.cdc_stream = CdcStreamingFactory()
DA.paths.silver_source = f"{DA.hidden.datasets}/pii/silver"

DA.conclude_setup()

