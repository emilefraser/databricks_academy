# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Load Data to Multiplex Bronze
# MAGIC 
# MAGIC The chief architect has decided that rather than connecting directly to Kafka, a source system will send raw records as JSON files to cloud object storage. In this notebook, you'll build a multiplex table that will ingest these records with Auto Loader and store the entire history of this incremental feed. The initial table will store data from all of our topics and have the following schema. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC | topic | STRING |
# MAGIC | partition | LONG |
# MAGIC | offset | LONG
# MAGIC | timestamp | LONG |
# MAGIC | date | DATE |
# MAGIC | week_part | STRING |
# MAGIC 
# MAGIC This single table will drive the majority of the data through the target architecture, feeding three interdependent data pipelines.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC 
# MAGIC **NOTE**: Details on additional configurations for connecting to Kafka are available <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">here</a>.
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe a multiplex design
# MAGIC - Apply Auto Loader to incrementally process records
# MAGIC - Configure trigger intervals
# MAGIC - Use "trigger-available-now" logic to execute triggered incremental loading of data.

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell declares the paths needed throughout this notebook.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.1

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> All records are being stored on the DBFS root for this training example.
# MAGIC 
# MAGIC Setting up separate databases and storage accounts for different layers
# MAGIC of data is preferred in both development and production environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Source Data
# MAGIC 
# MAGIC Data files are being written to the path specified by the variable below.
# MAGIC 
# MAGIC Run the following cell to examine the schema in the source data and determine if anything needs to be changed as it's being ingested.

# COMMAND ----------

spark.read.json(DA.paths.source_daily).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data to Join with Date Lookup Table
# MAGIC The initialization script has loaded a **`date_lookup`** table. This table has a number of pre-computed date values. Note that additional fields indicating holidays or financial quarters might often be added to this table for later data enrichment.
# MAGIC 
# MAGIC Pre-computing and storing these values is especially important based on our desire to partition our data by year and week, using the string pattern **`YYYY-WW`**. While Spark has both **`year`** and **`weekofyear`** functions built in, the **`weekofyear`** function may not provide expected behavior for dates falling in the last week of December or <a href="https://spark.apache.org/docs/2.3.0/api/sql/#weekofyear" target="_blank">first week of January</a>, as it defines week 1 as the first week with >3 days.
# MAGIC 
# MAGIC While this edge case is esoteric to Spark, a **`date_lookup`** table that will be used across the organization is important for making sure that data is consistently enriched with date-related details.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE date_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC The current table being implemented requires that we capture the accurate **`week_part`** for each **`date`**.
# MAGIC 
# MAGIC The following call creates the **`DataFrame`** needed for the subsequent join operation.

# COMMAND ----------

date_lookup_df = spark.table("date_lookup").select("date", "week_part")

# COMMAND ----------

# MAGIC %md
# MAGIC Working with the JSON data stored in the **`DA.paths.source_daily`** location, transform the **`timestamp`** column as necessary to join it with the **`date`** column.

# COMMAND ----------

# TODO
from pyspark.sql import functions as F
json_df = spark.read.json(DA.paths.source_daily)
 
joined_df = (json_df.join(F.broadcast(date_lookup_df),
                          FILL_IN,  # Insert the matching condition
                          "left"))
 
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Triggered Incremental Auto Loading to Multiplex Bronze Table
# MAGIC 
# MAGIC Below is starter code for a function to incrementally process data from the source directory to the bronze table, creating the table during the initial write.
# MAGIC 
# MAGIC Fill in the missing code to:
# MAGIC - Configure the stream to use Auto Loader
# MAGIC - Configure Auto Loader to use the JSON format
# MAGIC - Perform a broadcast join with the date_lookup table
# MAGIC - Partition the data by the **`topic`** and **`week_part`** fields

# COMMAND ----------

# TODO
def process_bronze():
    query = (spark.readStream
                  .FILL_IN
                  .FILL_IN
                  .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze_schema")
                  .load(DA.paths.source_daily)
                  .join(F.broadcast(date_lookup_df), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                  .partitionBy(FILL_IN)
                  .trigger(availableNow=True)
                  .table("bronze"))
 
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to process an incremental batch of data.

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC Review the count of processed records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Preview the data to ensure records are being ingested correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The **`DA.daily_stream.load()`** code below is a helper class to land new data in the source directory.
# MAGIC 
# MAGIC Executing the following cell should successfully process a new batch.

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the count is now higher.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
