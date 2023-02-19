# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="6.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.paths.raw_user_reg = f"{DA.paths.user_db}/pii/raw_user_reg"

class UserRegStreamingFactory:
    def __init__(self, starting_batch=0, max_batch=15):
        self.batch = starting_batch
        self.max_batch = max_batch

    def load_batch(self, batches):
        from pyspark.sql import functions as F
        
        df = (spark.read
              .format("json")
              .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
              .load(f"{DA.hidden.datasets}/user-reg")
              .withColumn("date", F.col("registration_timestamp").cast("timestamp").cast("date"))
              .withColumn("batch", F.when(F.col("date") < "2019-12-01", F.lit(0)).otherwise(F.dayofmonth(F.col("date"))))
              .drop("date")
              .filter(F.col("batch").isin(batches))
              .drop("batch")
              .cache())

        df.write.mode("append").format("json").save(DA.paths.raw_user_reg)
        return df.count()
        
    def load(self, continuous=False):
        import time
        from pyspark.sql import functions as F

        start = int(time.time())

        if self.batch > self.max_batch:
            print("Data source exhausted\n")
            
        elif not continuous:
            print(f"Loading batch #{self.batch} to raw_user_reg", end="...")
            total = self.load_batch([self.batch])
            self.batch += 1
            
        else:
            print("Loading all batches to raw_user_reg", end="...")
            batches = list(range(self.batch, self.max_batch+1))
            total = self.load_batch(batches)
            self.batch = self.max_batch+1
            
        print(f"({int(time.time())-start} seconds, {total:,} records)")

        spark.sql("DROP TABLE IF EXISTS final_df")
            
DA.user_reg_stream = UserRegStreamingFactory()

# COMMAND ----------

DA.cleanup()
DA.init()

DA.create_bronze_table()
print()

DA.user_reg_stream.load()

# Stores salt="beans" in the secrets store, if permitted.  Exceptions are suppressed.
DA.databricks_api('POST', '2.0/secrets/scopes/create', on_error='return', scope="DA-ADE3.03", initial_manage_principal="users")
DA.databricks_api('POST', '2.0/secrets/put',           on_error='return', scope="DA-ADE3.03", key="salt", string_value="BEANS")

DA.conclude_setup()

None

