# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="e6dedae8-1335-494e-acdf-4a1906f8c826"/>
# MAGIC 
# MAGIC 
# MAGIC # Using Auto Loader and Structured Streaming with Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Ingest data using Auto Loader
# MAGIC * Aggregate streaming data
# MAGIC * Stream data to a Delta table

# COMMAND ----------

# MAGIC %md <i18n value="ab5018b7-17b9-4f66-a32d-9c86860f6f30"/>
# MAGIC 
# MAGIC 
# MAGIC ## Setup
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06.3L

# COMMAND ----------

# MAGIC %md <i18n value="03347519-151b-4304-8cda-1cbd91af0737"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Configure Streaming Read
# MAGIC 
# MAGIC This lab uses a collection of customer-related CSV data from the **retail-org/customers** dataset.
# MAGIC 
# MAGIC Read this data using <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Create a streaming temporary view called **`customers_raw_temp`**.

# COMMAND ----------

# ANSWER
dataset_source = f"{DA.paths.datasets}/retail-org/customers/"
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", customers_checkpoint_path)
      .load(dataset_source)
      .createOrReplaceTempView("customers_raw_temp"))

# COMMAND ----------

from pyspark.sql import Row
assert Row(tableName="customers_raw_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customers_raw_temp").dtypes ==  [('customer_id', 'string'),
 ('tax_id', 'string'),
 ('tax_code', 'string'),
 ('customer_name', 'string'),
 ('state', 'string'),
 ('city', 'string'),
 ('postcode', 'string'),
 ('street', 'string'),
 ('number', 'string'),
 ('unit', 'string'),
 ('region', 'string'),
 ('district', 'string'),
 ('lon', 'string'),
 ('lat', 'string'),
 ('ship_to_address', 'string'),
 ('valid_from', 'string'),
 ('valid_to', 'string'),
 ('units_purchased', 'string'),
 ('loyalty_segment', 'string'),
 ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="4582665f-8192-4751-83f8-8ae1a4d55f22"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Define a streaming aggregation
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_by_state_temp`** that counts the number of customers per **`state`**, in a field called **`customer_count`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC SELECT
# MAGIC   state,
# MAGIC   count(customer_id) AS customer_count
# MAGIC   FROM customers_raw_temp
# MAGIC   GROUP BY
# MAGIC   state

# COMMAND ----------

assert Row(tableName="customer_count_by_state_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state_temp").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="bef919d7-d681-4233-8da5-39ca94c49a8b"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Write aggregated data to a Delta table
# MAGIC 
# MAGIC Stream data from the **`customer_count_by_state_temp`** view to a Delta table called **`customer_count_by_state`**.

# COMMAND ----------

# ANSWER
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"

query = (spark.table("customer_count_by_state_temp")
              .writeStream
              .format("delta")
              .option("checkpointLocation", customers_count_checkpoint_path)
              .outputMode("complete")
              .table("customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

assert Row(tableName="customer_count_by_state", isTemporary=False) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md <i18n value="f74f262f-10c4-4f2f-84d6-f69e56c54ac6"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM customer_count_by_state

# COMMAND ----------

# MAGIC %md <i18n value="e2cf644d-96f9-47f7-ad81-780125d3ad4b"/>
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Run the following cell to remove the database and all data associated with this lab.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md <i18n value="8f3c4c52-b5d9-4f8a-974c-ce5db6430c43"/>
# MAGIC 
# MAGIC 
# MAGIC By completing this lab, you should now feel comfortable:
# MAGIC * Using PySpark to configure Auto Loader for incremental data ingestion
# MAGIC * Using Spark SQL to aggregate streaming data
# MAGIC * Streaming data to a Delta table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
