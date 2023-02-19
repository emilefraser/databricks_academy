-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="d2c3cdc2-5fcf-4edc-8101-2964a9355000"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Extract and Load Data Lab
-- MAGIC 
-- MAGIC In this lab, you will extract and load raw data from JSON files into a Delta table.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create an external table to extract data from JSON files
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files

-- COMMAND ----------

-- MAGIC %md <i18n value="e261fd97-ffd7-44b2-b1ca-61b843ee8961"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.5L

-- COMMAND ----------

-- MAGIC %md <i18n value="d7759322-f9b9-4abe-9b30-25f5a7e30d9c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC We will work with a sample of raw Kafka data written as JSON files. 
-- MAGIC 
-- MAGIC Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file. 
-- MAGIC 
-- MAGIC The schema for the table:
-- MAGIC 
-- MAGIC | field  | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | timestamp | LONG    | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |

-- COMMAND ----------

-- MAGIC %md <i18n value="f2cd70fe-65a1-4dce-b264-c0c7d225640a"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Extract Raw Events From JSON Files
-- MAGIC To load this data into Delta properly, we first need to extract the JSON data using the correct schema.
-- MAGIC 
-- MAGIC Create an external table against JSON files located at the filepath provided below. Name this table **`events_json`** and declare the schema above.

-- COMMAND ----------

-- ANSWER
CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON 
OPTIONS (path = "${da.paths.datasets}/ecommerce/raw/events-kafka")

-- COMMAND ----------

-- MAGIC %md <i18n value="07ce3850-fdc7-4dea-9335-2a093c2e200c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return an error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %md <i18n value="ae3b8554-d0e7-4fd7-b25a-27bfbc5f7c13"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Insert Raw Events Into Delta Table
-- MAGIC Create an empty managed Delta table named **`events_raw`** using the same schema.

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY);

-- COMMAND ----------

-- MAGIC %md <i18n value="3d56975b-47ba-4678-ae7b-7c5e4ac20a97"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md <i18n value="61815a62-6d4f-47fb-98a9-73c39842ac56"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Once the extracted data and Delta table are ready, insert the JSON records from the **`events_json`** table into the new **`events_raw`** Delta table.

-- COMMAND ----------

-- ANSWER
INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- MAGIC %md <i18n value="4f545052-31c6-442b-a5e8-4c5892ec912f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- ANSWER
SELECT * FROM events_raw

-- COMMAND ----------

-- MAGIC %md <i18n value="0d66f26b-3df6-4819-9d84-22da9f55aeaa"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the data has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC 
-- MAGIC first_5 = [row['timestamp'] for row in spark.table("events_raw").select("timestamp").orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC assert first_5 == [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], "Make sure you have not modified the data provided"
-- MAGIC 
-- MAGIC last_5 = [row['timestamp'] for row in spark.table("events_raw").select("timestamp").orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC assert last_5 == [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="e9565088-1762-4f89-a06f-49576a53526a"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create Delta Table from a Query
-- MAGIC In addition to new events data, let's also load a small lookup table that provides product details that we'll use later in the course.
-- MAGIC Use a CTAS statement to create a managed Delta table named **`item_lookup`** that extracts data from the parquet directory provided below.

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE item_lookup 
AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- MAGIC %md <i18n value="9f1ad20f-1238-4a12-ad2a-f10169ed6475"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the lookup table has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").orderBy('item_id').limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="c24885ea-010e-4b76-9e9d-cc749f10993a"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
