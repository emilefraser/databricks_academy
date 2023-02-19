# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Partitioning
# MAGIC ##### Objectives
# MAGIC 1. Get partitions and cores
# MAGIC 1. Repartition DataFrames
# MAGIC 1. Configure default shuffle partitions
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`repartition`**, **`coalesce`**, **`rdd.getNumPartitions`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html" target="_blank">SparkConf</a>: **`get`**, **`set`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html" target="_blank">SparkSession</a>: **`spark.sparkContext.defaultParallelism`**
# MAGIC 
# MAGIC ##### SparkConf Parameters
# MAGIC - **`spark.sql.shuffle.partitions`**, **`spark.sql.adaptive.enabled`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### Get partitions and cores
# MAGIC 
# MAGIC Use the **`rdd`** method **`getNumPartitions`** to get the number of DataFrame partitions.

# COMMAND ----------

df = spark.read.format("delta").load(DA.paths.events)
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md Access **`SparkContext`** through **`SparkSession`** to get the number of cores or slots.
# MAGIC 
# MAGIC Use the **`defaultParallelism`** attribute to get the number of cores in a cluster.

# COMMAND ----------

print(spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md **`SparkContext`** is also provided in Databricks notebooks as the variable **`sc`**.

# COMMAND ----------

print(sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md ### Repartition DataFrame
# MAGIC 
# MAGIC There are two methods available to repartition a DataFrame: **`repartition`** and **`coalesce`**.

# COMMAND ----------

# MAGIC %md #### **`repartition`**
# MAGIC Returns a new DataFrame that has exactly **`n`** partitions.
# MAGIC 
# MAGIC - Wide transformation
# MAGIC - Pro: Evenly balances partition sizes  
# MAGIC - Con: Requires shuffling all data

# COMMAND ----------

repartitioned_df = df.repartition(8)

# COMMAND ----------

repartitioned_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md #### **`coalesce`**
# MAGIC Returns a new DataFrame that has exactly **`n`** partitions, when fewer partitions are requested.
# MAGIC 
# MAGIC If a larger number of partitions is requested, it will stay at the current number of partitions.
# MAGIC 
# MAGIC - Narrow transformation, some partitions are effectively concatenated
# MAGIC - Pro: Requires no shuffling
# MAGIC - Cons:
# MAGIC   - Is not able to increase # partitions
# MAGIC   - Can result in uneven partition sizes

# COMMAND ----------

coalesce_df = df.coalesce(8)

# COMMAND ----------

coalesce_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md ### Configure default shuffle partitions
# MAGIC 
# MAGIC Use the SparkSession's **`conf`** attribute to get and set dynamic Spark configuration properties. The **`spark.sql.shuffle.partitions`** property determines the number of partitions that result from a shuffle. Let's check its default value:

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md 
# MAGIC Assuming that the data set isn't too large, you could configure the default number of shuffle partitions to match the number of cores:

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
print(spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md ### Partitioning Guidelines
# MAGIC - Make the number of partitions a multiple of the number of cores
# MAGIC - Target a partition size of ~200MB
# MAGIC - Size default shuffle partitions by dividing largest shuffle stage input by the target partition size (e.g., 4TB / 200MB = 20,000 shuffle partition count)
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> When writing a DataFrame to storage, the number of DataFrame partitions determines the number of data files written. (This assumes that <a href="https://sparkbyexamples.com/apache-hive/hive-partitions-explained-with-examples/" target="_blank">Hive partitioning</a> is not used for the data in storage. A discussion of DataFrame partitioning vs Hive partitioning is beyond the scope of this class.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adaptive Query Execution
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/partitioning_aqe.png" width="60%" />
# MAGIC 
# MAGIC In Spark 3, <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution" target="_blank">AQE</a> is now able to <a href="https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html" target="_blank"> dynamically coalesce shuffle partitions</a> at runtime. This means that you can set **`spark.sql.shuffle.partitions`** based on the largest data set your application processes and allow AQE to reduce the number of partitions automatically when there is less data to process.
# MAGIC 
# MAGIC The **`spark.sql.adaptive.enabled`** configuration option controls whether AQE is turned on/off.

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
