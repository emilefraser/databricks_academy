# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Scheduling Efficient Structured Streaming Jobs
# MAGIC 
# MAGIC We'll use this notebook as a framework to launch multiple streams on shared resources.
# MAGIC 
# MAGIC This notebook contains partially refactored code with all the updates and additions that will allow us to schedule our pipelines and run them as new data arrives, including logic for dealing with partition deletes from our **`bronze`** table.
# MAGIC 
# MAGIC Also included is logic to assign each stream to a scheduler pool. Review the code below and then follow the instructions in the following cell to schedule a streaming job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling this Notebook
# MAGIC 
# MAGIC This notebook is designed to be scheduled against a jobs cluster, but can use an interactive cluster to avoid cluster start up times. 
# MAGIC 
# MAGIC Note that executing additional code against an all purpose cluster will result in significant query slowdown.
# MAGIC 
# MAGIC The recomended cluster configuration for this demo includes:
# MAGIC * The latest LTS version of the DBR
# MAGIC * A single-node cluster
# MAGIC * A single VM with ~32 cores

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Shuffle Partitions
# MAGIC Because shuffles will be triggered by some workloads we need to manage the **`spark.sql.shuffle.partitions`**.
# MAGIC 
# MAGIC The default number of shuffle partitions (200) can cripple many streaming jobs.
# MAGIC 
# MAGIC As such, it's a reasonably good practice to simply use the maximum number of cores as the high end, and if smaller, maintain a factor of the number of course.
# MAGIC 
# MAGIC Naturally, this generalized advice changes as you increase the number of streams running on a single cluster.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note that this value cannot be changed between runs without creating a new checkpoint for each stream.

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC 
# MAGIC Jobs utilize the **`widgets`** submodule to pass parameters to notebooks.
# MAGIC 
# MAGIC The **`widgets`** submodule includes a number of methods to allow interactive variables to be set while working with notebooks in the workspace with an interactive cluster. To learn more about this functionality, refer to the <a href="https://docs.databricks.com/notebooks/widgets.html#widgets" target="_blank">Databricks documentation</a>.
# MAGIC 
# MAGIC This notebook will focus on only two of these methods, emphasizing their utility when running a notebook as a job:
# MAGIC 1. **`dbutils.widgets.text`** accepts a parameter name and a default value. This is the method through which external values can be passed into scheduled notebooks.
# MAGIC 1. **`dbutils.widgets.get`** accepts a parameter name and retrieves the associated value from the widget with that parameter name.
# MAGIC 
# MAGIC Taken together, **`dbutils.widgets.text`** allows the passing of external values and **`dbutils.widgets.get`** allows those values to be referenced.
# MAGIC 
# MAGIC **NOTE**: To run this notebook in triggered batch mode, pass key **`once`** and value **`True`** as a parameter to your scheduled job.

# COMMAND ----------

once = eval(dbutils.widgets.get("once"))
print(f"Once: {once}")

# COMMAND ----------

# MAGIC %md <trx-123>
# MAGIC # Use RocksDB for State Store
# MAGIC 
# MAGIC RocksDB efficiently managed state in the native memory and local SSD of the cluster, while also automatically saving changes to the provided checkpoint directory for each stream. While not necessary for all Structured Streaming jobs, it can be useful for queries with a large amount of state information being managed.
# MAGIC 
# MAGIC **NOTE**: The state management scheme cannot be changed between query restarts. Successful execution of this notebook requires that the checkpoints being used in the queries to be scheduled have been completely reset.

# COMMAND ----------

spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC The following cell loads variables and paths used throughout this notebook.
# MAGIC 
# MAGIC Note that the [Reset Pipelines]($./1 - Reset Pipelines) notebook included here should be run before scheduling jobs to ensure data is in a fresh state for testing.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Streaming Query Listener
# MAGIC 
# MAGIC Some production streaming applications require real-time monitoring of streaming query progress. 
# MAGIC 
# MAGIC Generally, these results will be streamed backed into a pub/sub system for real-time dashboarding. 
# MAGIC 
# MAGIC Here, we'll append the output logs to a JSON directory that we can later read in with Auto Loader.

# COMMAND ----------

# MAGIC %run ../../Includes/StreamingQueryListener

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Optimize and Auto Compaction
# MAGIC 
# MAGIC We'll want to ensure that our bronze table and 3 parsed silver tables don't contain too many small files. Turning on Auto Optimize and Auto Compaction help us to avoid this problem. For more information on these settings, <a href="https://docs.databricks.com/delta/optimizations/auto-optimize.html" target="_blank">consult our documentation</a>.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

date_lookup_df = spark.table("date_lookup").select("date", "week_part")

# COMMAND ----------

def process_bronze(source, table_name, checkpoint, once=False, processing_time="5 seconds"):
    from pyspark.sql import functions as F
    
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    
    data_stream_writer = (spark
            .readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilesPerTrigger", 2)
            .option("cloudFiles.format", "json")
            .load(source)
            .join(F.broadcast(date_lookup_df), [F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date")], "left")
            .writeStream
            .option("checkpointLocation", checkpoint)
            .partitionBy("topic", "week_part")
            .queryName("bronze")
         )
    
    if once == True:
        return data_stream_writer.trigger(availableNow=True).table(table_name)
    else:
        return data_stream_writer.trigger(processingTime=processing_time).table(table_name)
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Apache Spark Scheduler Pools for Efficiency
# MAGIC 
# MAGIC By default, all queries started in a notebook run in the same <a href="https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application" target="_blank">fair scheduling pool</a>. Therefore, jobs generated by triggers from all of the streaming queries in a notebook run one after another in first in, first out (FIFO) order. This can cause unnecessary delays in the queries, because they are not efficiently sharing the cluster resources.
# MAGIC 
# MAGIC In particular, resource-intensive streams can hog the available compute in a cluster, preventing smaller streams from achieving low latency. Configuring pools provides the capacity to fine tune your cluster to ensure processing time.
# MAGIC 
# MAGIC To enable all streaming queries to execute jobs concurrently and to share the cluster efficiently, you can set the queries to execute in separate scheduler pools. This **local property configuration** will be in the same notebook cell where we start the streaming query. For example:
# MAGIC 
# MAGIC ** Run streaming query1 in scheduler pool1 **
# MAGIC 
# MAGIC <strong><code>
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")<br/>
# MAGIC df.writeStream.queryName("query1").format("parquet").start(path1)
# MAGIC </code></strong>
# MAGIC 
# MAGIC ** Run streaming query2 in scheduler pool2 **
# MAGIC 
# MAGIC <strong><code>
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")<br/>
# MAGIC df.writeStream.queryName("query2").format("delta").start(path2)
# MAGIC </code></strong>

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze")

bronze_query = process_bronze(DA.paths.producer_30m, "bronze_dev", f"{DA.paths.checkpoints}/bronze", once=once)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parse Silver Tables
# MAGIC 
# MAGIC In the next cell, we define a Python class to handle the queries that result in our **`heart_rate_silver`** and **`workouts_silver`**.

# COMMAND ----------

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)

# COMMAND ----------

# heart_rate_silver
def heart_rate_silver(source_table="bronze", once=False, processing_time="10 seconds"):
    from pyspark.sql import functions as F
    
    query = """
        MERGE INTO heart_rate_silver a
        USING heart_rate_updates b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(query, "heart_rate_updates")
    
    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'bpm'")
        .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
        .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["device_id", "time"])
        .writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate_silver")
        .queryName("heart_rate_silver")
    )
  
    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

# workouts_silver
def workouts_silver(source_table="bronze", once=False, processing_time="15 seconds"):
    from pyspark.sql import functions as F
    
    query = """
        MERGE INTO workouts_silver a
        USING workout_updates b
        ON a.user_id=b.user_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(query, "workout_updates")
    
    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'workout'")
        .select(F.from_json(F.col("value").cast("string"), "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT").alias("v"))
        .select("v.*")
        .select("user_id", "workout_id", F.col("timestamp").cast("timestamp").alias("time"), "action", "session_id")
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["user_id", "time"])
        .writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/workouts_silver")
        .queryName("workouts_silver")

    )

    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

# users

def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

def users_silver(source_table="bronze", once=False, processing_time="30 seconds"):
    from pyspark.sql import functions as F

    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
        >"""

    salt = "BEANS"

    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'user_info'")
        .dropDuplicates()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                F.col('timestamp').cast("timestamp").alias("updated"),
                F.to_date('dob','MM/dd/yyyy').alias('dob'),
                'sex', 'gender','first_name','last_name',
                'address.*', "update_type")
        .writeStream
        .foreachBatch(batch_rank_upsert)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/users")
        .queryName("users")
    )
    
    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_parsed")

# COMMAND ----------

heart_rate_silver_query = heart_rate_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

workouts_silver_query = workouts_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

users_query = users_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

if once:
    # While triggered once, we still want them to run in
    #  parallel but, we want to block here until they are done.
    bronze_query.awaitTermination()
    heart_rate_silver_query.awaitTermination()
    workouts_silver_query.awaitTermination()
    users_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Unlike other lessons, we will **NOT** be be executing our **`DA.cleanup()`** command<br/>
# MAGIC as we want these assets to persist through all the notebooks in this demo.
# MAGIC 
# MAGIC However, we don't want to leave this demo running forever so we will stop all streams after 30 minutes.<br/>
# MAGIC This is approximately the same amount of time that our Streaming Factory will run from start to finish.

# COMMAND ----------

import time
if not once: 
    time.sleep(30*60)
    print("Time's up!")

# COMMAND ----------

# MAGIC %md And now that the 30 minutes have passed, we will stop all streams.

# COMMAND ----------

# If once, streams would have auto-terminated.
# Otherwise, we need to stop them now that our 30 min demo is over
for stream in spark.streams.active:
    print(f"Stopping the stream {stream.name}")
    stream.stop()
    stream.awaitTermination()
        
print("All done.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
