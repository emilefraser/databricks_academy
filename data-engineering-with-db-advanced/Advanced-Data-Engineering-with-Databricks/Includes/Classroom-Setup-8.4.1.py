# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="8.4"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def create_bronze_dev_table():
    import time
    
    start = int(time.time())
    print(f"Creating bronze_dev", end="...")

    spark.sql(f"""
      CREATE TABLE bronze_dev
      SHALLOW CLONE delta.`{DA.hidden.datasets}/bronze`
      LOCATION '{DA.paths.user_db}/bronze_dev'
    """)

    total = spark.read.table("bronze_dev").count()
    assert total == 10841978, f"Expected 10,841,978 records, found {total:,} in bronze_dev"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class BronzeDataStreamFactory:
    def __init__(self):
        pass
    
    def load_batch(self, df):
        df.drop("arrival").write.mode("append").format("json").save(DA.paths.producer_30m)
        return df.count()
    
    def load(self, from_batch=0, batch_delay=5):
        import time
        from pyspark.sql import functions as F

        total = 0
        batch = from_batch
        producer_df = spark.read.load(f"{DA.hidden.datasets}/kafka-30min")
        arrival_max, arrival_min = producer_df.select(F.max("arrival"), F.min("arrival")).collect()[0]

        if batch_delay == 0:
            start = int(time.time()*1000)
            print("Loading all batches to producer_30m", end="...")
            total = self.load_batch(producer_df.filter(F.col("arrival") >= arrival_min+batch))
            print(f"({int(time.time()*1000)-start:,} ms, {total:,} records)")
            
        else:
            while arrival_min+batch < arrival_max+1:
                start = int(time.time()*1000)
                print(f"Loading batch #{batch+1} to producer_30m", end="...")
                total += self.load_batch(producer_df.filter(F.col("arrival") == arrival_min+batch))
                print(f"({int(time.time()*1000)-start} ms, {total:,} records)")
                batch += 1    
                time.sleep(batch_delay)

DA.paths.producer_30m = f"{DA.paths.working_dir}/producer_30m"            
DA.bronze_data_stream = BronzeDataStreamFactory()

# COMMAND ----------

DA.cleanup()
DA.init()

create_date_lookup()
create_bronze_dev_table()

DA.conclude_setup()

