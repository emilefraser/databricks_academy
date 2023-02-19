# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Clone with Delta Lake
# MAGIC 
# MAGIC Delta Lake provides native support for copying existing tables with **`CLONE`**. This notebook will explore both deep and shallow clones. The docs for this feature are <a href="https://docs.databricks.com/delta/delta-utility.html#clone-a-delta-table" target="_blank">here</a>; full syntax docs are available <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-clone.html" target="_blank">here</a>.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the behavior of deep and shallow clones
# MAGIC * Use deep clones to create full incremental backups of tables
# MAGIC * Use shallow clones to create development datasets
# MAGIC * Describe expected behavior after performing common database operations on source and clone tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the Environment
# MAGIC The following cell will create a database and source table that we'll use in this lesson, alongside some variables we'll use to control file locations.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the Production Table Details
# MAGIC The production table we'll be using as our source is named **`sensors_prod`**.
# MAGIC 
# MAGIC Use the following cell to explore the table history. Note that 4 total transactions have been run to create and load data into this table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensors_prod

# COMMAND ----------

# MAGIC %md
# MAGIC Explore the table description to discover the schema and additional details. Note that comments have been added to describe each data field.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED sensors_prod

# COMMAND ----------

# MAGIC %md
# MAGIC The helper function **`DA.check_files`** was defined to accept a table name and return the count of underlying data files (as well as list the content of the table directory).
# MAGIC 
# MAGIC Recall that all Delta tables comprise:
# MAGIC 1. Data files stored in parquet format
# MAGIC 1. Transaction logs stored in the **`_delta_log`** directory
# MAGIC 
# MAGIC The table name we're interacting with in the metastore is just a pointer to these underlying assets.

# COMMAND ----------

files = DA.check_files("sensors_prod")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Backup of Your Dataset with Deep Clone
# MAGIC 
# MAGIC Deep clone will copy all data and metadata files from your source table to a specified location, registering it with the declared table name.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sensors_backup 
# MAGIC DEEP CLONE sensors_prod
# MAGIC LOCATION '${da.paths.working_dir}/backup/sensors'

# COMMAND ----------

# MAGIC %md
# MAGIC You'll recall that our **`sensors_prod`** table had 4 versions associated with it. The clone operation created version 0 of the cloned table. 
# MAGIC 
# MAGIC The **`operationsParameters`** field indicates the **`sourceVersion`** that was cloned.
# MAGIC 
# MAGIC The **`operationMetrics`** field will provide information about the files copied during this transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensors_backup

# COMMAND ----------

# MAGIC %md
# MAGIC Metadata like comments will also be cloned.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED sensors_backup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Cloning
# MAGIC 
# MAGIC If you examine the files in your backup table, you'll see that you have the same number of files as your source table. Upon closer examination, you'll note that file names and sizes have also been preserved by the clone. 
# MAGIC 
# MAGIC This allows Delta Lake to incrementally apply changes to the backup table.

# COMMAND ----------

files = DA.check_files("sensors_backup")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC To see incremental clone in action, begin by committing a transaction to the **`sensor_prod`** table. Here, we'll delete all those records where **`sensor_type`** is "C".
# MAGIC 
# MAGIC Remember that Delta Lake manages changes at the file level, so any file containing a matching record will be rewritten.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM sensors_prod WHERE sensor_type = 'C'

# COMMAND ----------

# MAGIC %md
# MAGIC When we re-execute our deep clone command, we only copy those files that were written during our most recent transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sensors_backup 
# MAGIC DEEP CLONE sensors_prod
# MAGIC LOCATION '${da.paths.working_dir}/backup/sensors'

# COMMAND ----------

# MAGIC %md
# MAGIC We can review our history to confirm this.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensors_backup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Development Datasets with Shallow Clone
# MAGIC 
# MAGIC Whereas deep clone copies both data and metadata, shallow clone just copies the metadata and creates a pointer to the existing data files.
# MAGIC 
# MAGIC Note that the cloned table will have read-only permissions on the source data files. This makes it easy to create development datasets using a production dataset without fear of table corruption.
# MAGIC 
# MAGIC Here, we'll also specify using version 2 of our source production table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sensors_dev
# MAGIC SHALLOW CLONE sensors_prod VERSION AS OF 2
# MAGIC LOCATION '${da.paths.working_dir}/dev/sensors'

# COMMAND ----------

# MAGIC %md
# MAGIC When we look at the target directory, we'll note that no data files exist. 
# MAGIC 
# MAGIC The metadata for this table just points to those data files in the source table's data directory.

# COMMAND ----------

files = DA.check_files("sensors_dev")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Changes to Development Data
# MAGIC But what happens if you want to test modifications to your development table?
# MAGIC 
# MAGIC The code below inserts only those records from version 3 of our production table that don't have the value "C" as a **`sensor_type`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sensors_dev dev
# MAGIC USING (SELECT * FROM sensors_prod@v3 WHERE sensor_type != "C") prod
# MAGIC ON dev.device_id = prod.device_id AND dev.time = prod.time
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC The operation is successful and new rows are inserted. If we check the contents of our table location, we'll see that data files now exists.

# COMMAND ----------

files = DA.check_files("sensors_dev")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Any changes made to a shallow cloned table will write new data files to the specified target directory, meaning that you can safely test writes, updates, and deletes without risking corruption of your original table. The Delta logs will automatically reference the correct files (from the source table and this clone directory) to materialize the current view of your dev table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Retention and Cloned Tables
# MAGIC 
# MAGIC It's important to understand how cloned tables behave with file retention actions.
# MAGIC 
# MAGIC Recall the files in our **`sensors_prod`** table:

# COMMAND ----------

files = DA.check_files("sensors_prod")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to **`VACUUM`** your source production table (removing all files not referenced in the most recent version).

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.sql("VACUUM sensors_prod RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md We see that there are now fewer total data files associated with this table.

# COMMAND ----------

files = DA.check_files("sensors_prod")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC You'll recall that our **`sensors_dev`** table was initialized against version 2 of our production table. As such, it still has reference to data files associated with that table version.
# MAGIC 
# MAGIC Because these data files have been removed by our vacuum operation, we should expect the following query against our shallow cloned table to fail.
# MAGIC 
# MAGIC Uncomment it now and give it a try:

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT * FROM sensors_dev

# COMMAND ----------

# MAGIC %md
# MAGIC Because deep clone created a full copy of our files and associated metadata, we still have access to our **`sensors_backup`** table. Here, we'll query the original version of this backup (which corresponds to version 3 of our source table).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensors_backup@v0

# COMMAND ----------

# MAGIC %md
# MAGIC One of the useful features of deep cloning is the ability to set different table properties for file and log retention. This allows production tables to have optimized performance while maintaining files for auditing and regulatory compliance. 
# MAGIC 
# MAGIC The cell below sets the log and deleted file retention periods to 10 years.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sensors_backup
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = '3650 days',
# MAGIC   delta.deletedFileRetentionDuration = '3650 days'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC In this notebook, we explored the basic syntax and behavior of deep and shallow clones. We saw how changes to source and clone tables impacted tables, including the ability to incrementally clone changes to keep a backup table in-sync with its source. We saw that shallow clone could be used for creating temporary tables for development based on production data, but noted that removal of source data files will lead to errors when trying to query this shallow clone.

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
