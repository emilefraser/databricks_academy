# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Understanding Delta Lake Transactions
# MAGIC 
# MAGIC To provide atomic, durable transactions in the Lakehouse, Delta Lake utilizes transaction logs stored alongside data files. This notebook dives into the transaction logs to demonstrate what information is recorded and explore how the current version of the table is materialized.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, student will be able to:
# MAGIC - Describe how Delta Lake tracks and manages transactions
# MAGIC - Define the atomicity and durability guarantees of Delta Lake
# MAGIC - Map transactional metadata back to table operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atomic, Durable Transactions in the Lakehouse
# MAGIC 
# MAGIC Briefly, Delta Lake transactions can be described as the following:
# MAGIC - All write operations commit data changes as Parquet files
# MAGIC - Transactions commit when JSON log files are written
# MAGIC - Logs are stored in a nested directory
# MAGIC - Both data and transaction logs inherit the durability guarantees of the file system
# MAGIC 
# MAGIC The cloud-based object storage used by Databricks provides the following advantages and guarantees:
# MAGIC - Infinitely scalable
# MAGIC - Affordable
# MAGIC - Availability:  > 99.9%
# MAGIC - Durability: > 99.999999999%
# MAGIC 
# MAGIC Note that the Databricks platform has SLAs separate from cloud vendor infrastructure, and the guarantees of each cloud vendor differ slightly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Table and Insert Records
# MAGIC 
# MAGIC The cell below contains 4 Delta Lake operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE bronze 
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC 
# MAGIC INSERT INTO bronze VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO bronze VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO bronze VALUES (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, the table has three rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Reviewing the history allows us to see how operations and versions are linked.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The transaction log directory is nested under the table directory and contains a log file for each transaction (alongside checksum files and other files used to manage consistency with cloud object storage).

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC The table was created without any data present, so the initial version only contains metadata about the table schema and the user, environment, and time of the transaction.
# MAGIC 
# MAGIC Note that the transaction log is a simple JSON file, which can be reviewed directly from the file or read with Spark.

# COMMAND ----------

display(spark.read.json(f"{DA.paths.user_db}/bronze/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below leverages a Databricks setting to automatically return the Delta transaction log for the most recent commit version.

# COMMAND ----------

def display_delta_log(table, version=None):
    if not version:
        version = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
    version_str = str(int(version)).zfill(20)
    file = f"{DA.paths.user_db}/{table}/_delta_log/{version_str}.json"
    print("Showing: "+file)
    display(spark.read.json(file))

# COMMAND ----------

# MAGIC %md
# MAGIC Here we'll see that a simple insert transaction results in new files being tracked in the **`add`** column alongside **`commitInfo`**.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appending Data
# MAGIC Note that when appending data, multiple data files may be written as part of a single transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (4, "Ted", 4.7),
# MAGIC        (5, "Tiffany", 5.5),
# MAGIC        (6, "Vini", 6.3)

# COMMAND ----------

# MAGIC %md
# MAGIC The **`add`** column below contains paths and stats for each of the files added to the table.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Most append operations will insert many records into a single file.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze SELECT * FROM new_records

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, multiple records appear in a single data file under the **`add`** column.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deleting Data
# MAGIC Deleting a single record from a file with multiple records will actually result in a file being added.

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM bronze WHERE name = "Viktor"

# COMMAND ----------

# MAGIC %md
# MAGIC The file added contains the other records that were in the same data file as the record deleted in the transaction. 
# MAGIC 
# MAGIC In this specific case, the **`remove`** column indicates that the previous file with 3 records is no longer valid; the **`add`** column points to a new file containing only 2 records.
# MAGIC 
# MAGIC This is the expected behavior, as Delta Lake does not modify data files in place.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updating Data
# MAGIC Anytime a record in an existing file is modified, a new file will be added, and the old file will be indicated in the **`remove`** column, as in the update below.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronze SET name = "Vincent" WHERE id = 6

# COMMAND ----------

# MAGIC %md
# MAGIC Here, both the file removed and the file rewritten contain only the modified record. In production environments, you are unlikely to see a data file containing a single record.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining Transactions with Merge
# MAGIC The Delta Lake **`MERGE`** syntax allows updates, deletes, and inserts to occur in a single transaction.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze b
# MAGIC USING updates u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the table history, note how **`MERGE`** operations differ from **`DELETE`**, **`UPDATE`**, or **`INSERT`** transactions.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The **`operationMetrics`** and **`operationParameters`** within the **`commitInfo`** describe the source data, the parameters for the query, and all the results of the transaction.
# MAGIC 
# MAGIC Use the query below to find:
# MAGIC - How many rows were in the **`updates`** table
# MAGIC - How many records were inserted
# MAGIC - How many records were deleted
# MAGIC - How many records were updated
# MAGIC - The total number of files removed
# MAGIC - The total number of files added

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Utility Functions
# MAGIC Delta Lake table utility functions modify the metadata of a table without changing underlying data. 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze
# MAGIC ALTER COLUMN name
# MAGIC COMMENT "User first name"

# COMMAND ----------

# MAGIC %md
# MAGIC Files will not be marked as added or removed, as only the metadata has been updated.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Compaction
# MAGIC Running **`OPTIMIZE`** on a table will compact small files toward the target file size.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE bronze

# COMMAND ----------

# MAGIC %md
# MAGIC During file compaction, many files should be marked as removed, while a smaller number of files will be marked as added.
# MAGIC 
# MAGIC No data or files are deleted during this operation; as always, adding files to the **`remove`** column means they will not be used when querying the current version of the table, but does not permanently delete them from the underlying storage.

# COMMAND ----------

display_delta_log("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Log Checkpoints
# MAGIC Databricks will automatically create Parquet checkpoint files at fixed intervals to accelerate the resolution of the current table state.
# MAGIC 
# MAGIC Version 10 of the table should have both a **`.json`** and a **`.checkpoint.parquet`** file associated with it.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Rather than only showing the operations of the most recent transaction, this checkpoint file condenses all of the **`add`** and **`remove`** instructions and valid **`metaData`** into a single file.
# MAGIC 
# MAGIC This means that rather than loading many JSON files and comparing files listed in the **`add`** and **`remove`** columns to find those data files that currently represent the valid table version, a single file can be loaded that fully describes the table state.
# MAGIC 
# MAGIC Transactions after a checkpoint leverage this starting point, resolved new info from JSON files with the instructions from this Parquet snapshot.

# COMMAND ----------

display(spark.read.parquet(f"{DA.paths.user_db}/bronze/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that all of the data files in both the **`add`** and **`remove`** columns are still present in the table directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Up Stale Data Files
# MAGIC Executing **`VACUUM`** performs garbage cleanup on this directory. By default, a retention threshold of 7 days will be enforced; here it is overridden to demonstrate permanent removal of data. Manually setting **`spark.databricks.delta.vacuum.logging.enabled`** to **`True`** ensures that this operation is also recorded in the transaction log. 
# MAGIC 
# MAGIC **NOTE**: Vacuuming a production table with a short retention can lead to data corruption and/or failure of long-running queries. 

# COMMAND ----------

spark.conf.set("spark.databricks.delta.vacuum.logging.enabled", True)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.sql("VACUUM bronze RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, a single file remains.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that both the start and end of the **`VACUUM`** operation are recorded in the history.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The **`VACUUM START`** version will record the number of files to be deleted, but does not contain a list of file names.
# MAGIC 
# MAGIC Once deleted, previous versions of the table relying on these files are no longer accessible.

# COMMAND ----------

display_delta_log("bronze", 11)

# COMMAND ----------

# MAGIC %md
# MAGIC Additional reading is available in this <a href="https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html" target="_blank">blog post</a>.

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
