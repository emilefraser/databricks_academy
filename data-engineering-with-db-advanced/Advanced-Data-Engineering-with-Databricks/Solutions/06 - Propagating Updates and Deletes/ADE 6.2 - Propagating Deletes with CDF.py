# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Propagating Deletes with Change Data Feed
# MAGIC 
# MAGIC While the PII for users has been pseudonymized, generalized, and redacted through several approaches, we have not yet addressed how deletes can be effectively and efficiently handled in the Lakehouse.
# MAGIC 
# MAGIC In this notebook, we'll combine Structured Streaming, Delta Lake, and Change Data Feed to demonstrate processing delete requests incrementally and propagating deletes through the Lakehouse.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_users.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Commit arbitrary messages to the Delta log to record important events
# MAGIC - Apply deletes using Delta Lake DDL
# MAGIC - Propagate deletes using Change Data Feed
# MAGIC - Leverage incremental syntax to ensure deletes are committed fully
# MAGIC - Describe default data retention settings for Change Data Feed

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements for Fulfilling Requests to Be Forgotten
# MAGIC 
# MAGIC The **`user_lookup`** table contains the link between the **`alt_id`** used as the primary key for the **`users`** table and natural keys found elsewhere in the lakehouse.
# MAGIC 
# MAGIC Different industries will have different requirements for data deletion and data retention. Here, we'll assume the following:
# MAGIC 1. All PII in the **`users`** table must be deleted
# MAGIC 1. Links between pseudonymized keys and natural keys should be forgotten
# MAGIC 1. A policy to remove historic data containing PII from raw data sources and logs should be enacted
# MAGIC 
# MAGIC This notebook will focus on the first two of these requirements; the third will be handled in the following lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Right to Be Forgotten Requests
# MAGIC 
# MAGIC While it is possible to process deletes at the same time as appends and updates, the fines around right to be forgotten requests may warrant a separate process.
# MAGIC 
# MAGIC Below, logic for setting up a simple table to process delete requests through the users data is displayed. A simple deadline of 30 days after the request is inserted, allowing internal automated audits to leverage this table to ensure compliance.

# COMMAND ----------

from pyspark.sql import functions as F

salt = "BEANS"

schema = """
    user_id LONG, 
    update_type STRING, 
    timestamp FLOAT, 
    dob STRING, 
    sex STRING, 
    gender STRING, 
    first_name STRING, 
    last_name STRING, 
    address STRUCT<street_address: STRING, 
                   city: STRING, 
                   state: STRING, 
                   zip: INT>"""

requests_df = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .dropDuplicates(["value"]) # Drop duplicate data, not just duplicate event deliveries.
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*", F.col('v.timestamp').cast("timestamp").alias("requested"))
                    .filter("update_type = 'delete'")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            "requested",
                            F.date_add("requested", 30).alias("deadline"), 
                            F.lit("requested").alias("status")))

# COMMAND ----------

# MAGIC %md
# MAGIC Preview the results of this operation.

# COMMAND ----------

display(requests_df, streamName = "requests")
DA.block_until_stream_is_ready(name = "requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Commit Messages
# MAGIC 
# MAGIC Delta Lake supports arbitrary commit messages that will be recorded to the Delta transaction log and viewable in the table history. This can help with later auditing.
# MAGIC 
# MAGIC Setting this with SQL will create a global commit message that will be used for all subsequent operations in our notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata=Deletes committed

# COMMAND ----------

# MAGIC %md
# MAGIC With DataFrames, commit messages can also be specified as part of the write options using the **`userMetadata`** option.
# MAGIC 
# MAGIC Here, we'll indicate that we're manually processing these requests in a notebook, rather than using an automated job.

# COMMAND ----------

query = (requests_df.writeStream
                    .outputMode("append")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/delete_requests")
                    .option("userMetadata", "Requests processed interactively")
                    .trigger(availableNow=True)
                    .table("delete_requests"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC These messages are clearly visible in the table history in the far right column.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Delete Requests
# MAGIC 
# MAGIC The **`delete_requests`** table will be used to track users' requests to be forgotten. Note that it is possible to process delete requests alongside inserts and updates to existing data as part of a normal **`MERGE`** statement.
# MAGIC 
# MAGIC Because PII exists in several places through the current lakehouse, tracking requests and processing them asynchronously may provide better performance for production jobs with low latency SLAs. The approach modeled here also indicates the time at which the delete was requested and the deadline, and provides a field to indicate the current processing status of the request.
# MAGIC 
# MAGIC Review the **`delete_requests`** table below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed to Power Incremental Deletes
# MAGIC 
# MAGIC We'll be using Change Data Feed to power deletes to many tables from a single source.
# MAGIC 
# MAGIC Because the **`user_lookup`** table links identifying information between different pipelines, we'll make this the point where deletes propagate from.
# MAGIC 
# MAGIC Start by altering the table properties to enable Change Data Feed.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE user_lookup 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm that Change Data Feed is enabled by looking at the table history.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC Note that because Change Data Feed was enabled after initial table creation, we will only be able to review change data starting with the current table version.
# MAGIC 
# MAGIC The cell below will capture this value for use in the next section.

# COMMAND ----------

start_version = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
print(start_version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Committing Deletes
# MAGIC When working with static data, committing deletes is simple. 
# MAGIC 
# MAGIC The following logic modifies the **`user_lookup`** table by rewriting all data files containing records affected by the **`DELETE`** statement. Recall that with Delta Lake, deleting data will create new data files rather than deleting existing data files.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM user_lookup
# MAGIC WHERE alt_id IN (SELECT alt_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propagate Deletes
# MAGIC While the lakehouse architecture implemented here typically uses the **`user_lookup`** as a static table in joins with incremental data, the Change Data Feed can be separately leveraged as an incremental record of data changes.
# MAGIC 
# MAGIC The code below configures as incremental read of all changes committed to the **`user_lookup`** table.

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", start_version)
                 .table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC The relationships between our natural keys (**`user_id`**, **`device_id`**, and **`mac_address`**) are stored in our **`user_lookup`**. These allow us to link a user's data between various pipelines/sources. The Change Data Feed from this table will maintain all these fields, allowing successful identification of records to be deleted or modified in downstream tables.
# MAGIC 
# MAGIC The function below demonstrates committing deletes to two tables using different keys and syntax. Note that in this case, the **`MERGE`** syntax demonstrated is not necessary to process the deletes to the **`users`** table; this code block does demonstrate the basic syntax that could be expanded if inserts and updates were to be processed in the same code block as deletes.
# MAGIC 
# MAGIC Assuming successful completion of these two table modifications, an update will be process back to the **`delete_requests`** table. Note that we're leveraging data that has been successfully deleted from the **`user_lookup`** table to update a value in the **`delete_requests`** table.

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING deletes d
        ON u.alt_id = d.alt_id
        WHEN MATCHED
            THEN DELETE
    """)

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM user_bins
        WHERE user_id IN (SELECT user_id FROM deletes)
    """)
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests dr
        USING deletes d
        ON d.alt_id = dr.alt_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that this workload is being driven by incremental changes to the **`user_lookup`** table (tracked through the Change Data Feed).
# MAGIC 
# MAGIC Executing the following cell will propagate deletes to a single table to multiple tables throughout the lakehouse.

# COMMAND ----------

query = (deleteDF.writeStream
                 .foreachBatch(process_deletes)
                 .outputMode("update")
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/deletes")
                 .trigger(availableNow=True)
                 .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Delete Commits
# MAGIC Note that with our current implementation, if a user registration never made it into the **`user_lookup`** table, data for this user will not be deleted from other tables. However, the status for these records in the **`delete_requests`** table will also remain **`requested`**, so a redundant approach could be applied if necessary.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC Note that our commit message will be in the far right column of our history, under the column **`userMetadata`**.
# MAGIC 
# MAGIC For the **`users`** table, the operation field in the history will indicate a merge because of the chosen syntax, even though only deletes were committed. The number of deleted rows can be reviewed in the **`operationMetrics`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY users

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, **`user_bins`** will show a delete.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_bins

# COMMAND ----------

# MAGIC %md
# MAGIC The changes to **`delete_requests`** also show a merge operation, and appropriately show that records have been updated rather than deleted in this table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Are Deletes Fully Committed?
# MAGIC 
# MAGIC Not exactly.
# MAGIC 
# MAGIC Because of how Delta Lake's history and CDF features are implemented, deleted values are still present in older versions of the data.
# MAGIC 
# MAGIC The query below shows the records deleted in v1 of the **`user_bins`** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_bins@v0 u1
# MAGIC EXCEPT 
# MAGIC SELECT * FROM user_bins u2

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly, while we've already applied our logic on the incremental data produced by deletes committed to the **`user_lookup`** table, this information is still available within the change feed.

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", start_version)
           .table("user_lookup")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC The next notebook will explore fully committing these deletes, as well as providing guidance for removing access to historic raw data containing PII.

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
