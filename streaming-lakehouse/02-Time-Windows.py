# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with Time Windows
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Explain why some methods will not work on streaming data
# MAGIC * Use windows to aggregate over chunks of data rather than all data
# MAGIC * Compare Tumbling Windows and Sliding Windows
# MAGIC * Apply watermarking to manage state
# MAGIC * Plot live graphs using `display`
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC The source contains smartphone accelerometer samples from devices and users with the following columns:
# MAGIC 
# MAGIC | Field          | Description |
# MAGIC | ------------- | ----------- |
# MAGIC | Arrival_Time | time data was received |
# MAGIC | Creation_Time | event time |
# MAGIC | Device | type of Model |
# MAGIC | Index | unique identifier of event |
# MAGIC | Model | i.e Nexus4  |
# MAGIC | User | unique user identifier |
# MAGIC | geolocation | city & country |
# MAGIC | gt | transportation mode |
# MAGIC | id | unused null field |
# MAGIC | x | acceleration in x-dir |
# MAGIC | y | acceleration in y-dir |
# MAGIC | z | acceleration in z-dir |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/classic-setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define the Schema and Configure Streaming Read
# MAGIC 
# MAGIC This lesson uses the same data as the previous notebook. The logic below defines the path and schema, sets up a streaming read, and selects only those fields that will be examined in this notebook. Note that the `Creation_Time` field had been encoded in nanonseconds and is being converted back to unixtime.

# COMMAND ----------

from pyspark.sql.functions import col

inputPath = "/mnt/training/definitive-guide/data/activity-json/streaming"

schema = "Arrival_Time BIGINT, Creation_Time BIGINT, Device STRING, Index BIGINT, Model STRING, User STRING, geolocation STRUCT<city: STRING, country: STRING>, gt STRING, id BIGINT, x DOUBLE, y DOUBLE, z DOUBLE"

streamingDF = (spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1) 
  .load(inputPath)
  .select((col("Creation_Time")/1E9).alias("time").cast("timestamp"),
        col("gt").alias("action"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsupported Operations
# MAGIC 
# MAGIC Most operations on a streaming DataFrame are identical to a static DataFrame. There are [some exceptions to this](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations).
# MAGIC 
# MAGIC Consider the model of the data as a constantly appending table. Sorting is one of a handful of operations that is either too complex or logically not possible to do when working with streaming data.

# COMMAND ----------

from pyspark.sql.functions import col

try:
  sortedDF = streamingDF.orderBy(col("time").desc())
  display(sortedDF)
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Streaming Aggregations
# MAGIC 
# MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
# MAGIC 
# MAGIC Some examples include
# MAGIC * Aggregating errors in data from IoT devices by type
# MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country.
# MAGIC * Doing behavior analysis on instant messages via hash tags.
# MAGIC 
# MAGIC While these streaming aggregates may need to reference historic trends, generally analytics will be calculated over discrete units of time. Spark Structured Streaming supports time-based **windows** on streaming DataFrames to make these calculations easy.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is Time?
# MAGIC 
# MAGIC Multiple times may be associated with each streaming event. Consider the discrete differences between the time at which the event data was:
# MAGIC - Generated
# MAGIC - Written to the streaming source
# MAGIC - Processed into Spark
# MAGIC 
# MAGIC Each of these times will be recorded from the system clock of the machine running the process. Discrepancies and latencies may have many different causes. 
# MAGIC 
# MAGIC Generally speaking, most analytics will be interested in the time the data was generated. As such, this lesson will focus on timestamps recorded at the time of data generation, here referred to as the **event time**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Windowing
# MAGIC 
# MAGIC Defining windows on a time series field allows users to utilize this field for aggregations in the same way they would use distinct values when calling `GROUP BY`. The state table will maintain aggregates for each user-defined bucket of time. Spark supports two types of windows:
# MAGIC 
# MAGIC **Tumbling Windows**
# MAGIC 
# MAGIC Windows do not overlap, but rather represent distinct buckets of time. Each event will be aggregated into only one window. 
# MAGIC 
# MAGIC **Sliding windows** 
# MAGIC 
# MAGIC The windows overlap and a single event may be aggregated into multiple windows. 
# MAGIC 
# MAGIC The diagram below from the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a> guide shows sliding windows.
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define a Windowed Aggregation
# MAGIC 
# MAGIC The method `window` accepts a timestamp column and a window duration to define tumbling windows. Adding a third argument for `slideDuration` allows definition of a sliding window; see [documentation](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=window#pyspark.sql.functions.window) for more details.
# MAGIC 
# MAGIC Here, the count of actions for each hour is aggregated.

# COMMAND ----------

from pyspark.sql.functions import window, col

countsDF = (streamingDF
  .groupBy(col("action"),                     
           window(col("time"), "1 hour"))    
  .count()                                    
  .select(col("window.start").alias("start"), 
          col("action"),                     
          col("count"))                      
  .orderBy(col("start"), col("action"))      
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Considerations
# MAGIC Because aggregation will trigger a shuffle, configuring the number of partitions can reduce the number of tasks and properly balance the workload for the cluster.
# MAGIC 
# MAGIC In most cases, a 1-to-1 mapping of partitions to cores is ideal for streaming applications. The code below sets the number of partitions to 8, which maps perfectly to a cluster with 8 cores.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### View and Plot Results
# MAGIC 
# MAGIC To view the results of our query, pass the DataFrame `countsDF` to the `display()` function.
# MAGIC 
# MAGIC Once the data is loaded, render a line graph with
# MAGIC * **Keys** is set to `start`
# MAGIC * **Series groupings** is set to `action`
# MAGIC * **Values** is set to `count`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember that calling `display` starts a stream with `memory` as the sink. In production, this would be written to a durable sink using the `complete` output mode.

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all Streams
# MAGIC 
# MAGIC When you are done, stop all the streaming jobs.

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Watermarking
# MAGIC 
# MAGIC By default, Structured Streaming keeps around only the minimal intermediate state required to update the results table. When aggregating with many buckets over a long running stream, this can lead to slowdown and eventually OOM errors as the number of buckets calculated with each trigger grows.
# MAGIC 
# MAGIC **Watermarking** allows users to define a cutoff threshold for how much state should be maintained. This cutoff is calculated against the max event time seen by the engine (i.e., the most recent event). Late arriving data outside of this threshold will be discarded.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run a Stream with Watermarking
# MAGIC 
# MAGIC The `withWatermark` option allows users to easily define this cutoff threshold.

# COMMAND ----------

watermarkedDF = (streamingDF
  .withWatermark("time", "2 hours")           # Specify a 2-hour watermark
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For each aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time
)
display(watermarkedDF)                        # Start the stream and display it

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Example Details
# MAGIC 
# MAGIC The threshold is always calculated against the max event time seen.
# MAGIC 
# MAGIC In the example above,
# MAGIC * The in-memory state is limited to two hours of historic data.
# MAGIC * Data arriving more than 2 hours late should be dropped.
# MAGIC * Data received within 2 hours of being generated will never be dropped.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This guarantee is strict in only one direction. Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated. The more delayed the data is, the less likely the engine is going to process it.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all the streams

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC - A handful of operations valid for static DataFrames will not work with streaming data
# MAGIC - Windows allow users to define time-based buckets for aggregating streaming data
# MAGIC - Watermarking allows users to manage the amount of state being calculated with each trigger and define how late-arriving data should be handled

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
