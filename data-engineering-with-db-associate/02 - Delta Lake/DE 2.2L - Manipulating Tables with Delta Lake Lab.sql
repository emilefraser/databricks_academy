-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="a209ac48-08a6-4b89-b728-084a515fd335"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # Manipulating Tables with Delta Lake
-- MAGIC 
-- MAGIC This notebook provides a hands-on review of some of the basic functionality of Delta Lake.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Execute standard operations to create and manipulate Delta Lake tables, including:
-- MAGIC   - **`CREATE TABLE`**
-- MAGIC   - **`INSERT INTO`**
-- MAGIC   - **`SELECT FROM`**
-- MAGIC   - **`UPDATE`**
-- MAGIC   - **`DELETE`**
-- MAGIC   - **`MERGE`**
-- MAGIC   - **`DROP TABLE`**

-- COMMAND ----------

-- MAGIC %md <i18n value="6582dbcd-72c7-496b-adbd-23aef98e20e9"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.2L

-- COMMAND ----------

-- MAGIC %md <i18n value="0607f2ed-cfe6-4a38-baa4-e6754ec1c664"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Table
-- MAGIC 
-- MAGIC In this notebook, we'll be creating a table to track our bean collection.
-- MAGIC 
-- MAGIC Use the cell below to create a managed Delta Lake table named **`beans`**.
-- MAGIC 
-- MAGIC Provide the following schema:
-- MAGIC 
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="2167d7d7-93d1-4704-a7cb-a0335eaf8da7"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return as error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md <i18n value="89004ef0-db16-474b-8cce-eff85c225a65"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Insert Data
-- MAGIC 
-- MAGIC Run the following cell to insert three rows into the table.

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md <i18n value="48d649a1-cde1-491f-a90d-95d2e336e140"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="f0406eef-6973-47c9-8f89-a667c53cfea7"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Insert the additional records provided below. Make sure you execute this as a single transaction.

-- COMMAND ----------

-- TODO
<FILL-IN>
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md <i18n value="e1764f9f-8052-47bb-a862-b52ca438378a"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the data is in the proper state.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md <i18n value="e38adafa-bd10-4191-9c69-e6a4363532ec"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Update Records
-- MAGIC 
-- MAGIC A friend is reviewing your inventory of beans. After much debate, you agree that jelly beans are delicious.
-- MAGIC 
-- MAGIC Run the following cell to update this record.

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md <i18n value="d8637fab-6d23-4458-bc08-ff777021e30c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC You realize that you've accidentally entered the weight of your pinto beans incorrectly.
-- MAGIC 
-- MAGIC Update the **`grams`** column for this record to the correct weight of 1500.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="954bf892-4db6-4b25-9a9a-83b0f6ecc123"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm this has completed properly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md <i18n value="f36d551a-f588-43e4-84a2-6aa49a420c04"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Delete Records
-- MAGIC 
-- MAGIC You've decided that you only want to keep track of delicious beans.
-- MAGIC 
-- MAGIC Execute a query to drop all beans that are not delicious.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="1c8d924c-3e97-49a0-b5e4-0378c5acd3c8"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to confirm this operation was successful.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md <i18n value="903473f1-ddca-41ea-ae2f-dc2fac64936e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Using Merge to Upsert Records
-- MAGIC 
-- MAGIC Your friend gives you some new beans. The cell below registers these as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md <i18n value="58d50e50-65f1-403b-b74e-1143cde49356"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC In the cell below, use the above view to write a merge statement to update and insert new records to your **`beans`** table as one transaction.
-- MAGIC 
-- MAGIC Make sure your logic:
-- MAGIC - Matches beans by name **and** color
-- MAGIC - Updates existing beans by adding the new weight to the existing weight
-- MAGIC - Inserts new beans only if they are delicious

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="9fbb65eb-9119-482f-ab77-35e11af5fb24"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC last_version = spark.sql("DESCRIBE HISTORY beans").orderBy(F.col("version").desc()).first()
-- MAGIC 
-- MAGIC assert last_version["operation"] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC 
-- MAGIC metrics = last_version["operationMetrics"]
-- MAGIC assert metrics["numOutputRows"] == "5", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md <i18n value="4a668d7c-e16b-4061-a5b7-1ec732236308"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Dropping Tables
-- MAGIC 
-- MAGIC When working with managed Delta Lake tables, dropping a table results in permanently deleting access to the table and all underlying data files.
-- MAGIC 
-- MAGIC **NOTE**: Later in the course, we'll learn about external tables, which approach Delta Lake tables as a collection of files and have different persistence guarantees.
-- MAGIC 
-- MAGIC In the cell below, write a query to drop the **`beans`** table.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md <i18n value="4cc5c126-5e56-423e-a814-f6c422312802"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to assert that your table no longer exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md <i18n value="f4d330e3-dc40-4b6e-9911-34902bab22ae"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Wrapping Up
-- MAGIC 
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands

-- COMMAND ----------

-- MAGIC %md <i18n value="d59f9828-9b13-4e0e-ae98-7e852cd32198"/>
-- MAGIC 
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
