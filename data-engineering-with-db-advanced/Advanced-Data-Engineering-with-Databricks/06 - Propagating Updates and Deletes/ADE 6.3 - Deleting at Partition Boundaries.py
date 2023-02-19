# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Deleting at Partition Boundaries
# MAGIC 
# MAGIC While we've deleted our PII from our silver tables, we haven't dealt with the fact that this data still exists in our **`bronze`** table.
# MAGIC 
# MAGIC Note that because of stream composability and the design choice to use a multiplex bronze pattern, enabling Delta Change Data Feed (CDF) to propagate delete information would require redesigning each of our pipelines to take advantage of this output. Without using CDF, modification of data in a table will break downstream composability.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC 
# MAGIC In this notebook, you'll learn how to delete partitions of data from Delta Tables and how to configure incremental reads to allow for these deletes.
# MAGIC 
# MAGIC This functionality is not only useful for permanently deleting PII, but this same pattern can be applied in companies that just want to expunge data older than a certain age from a given table. Similarly, data could be backed up to a cheaper storage tier, and then safely deleted from "active" or "hot" Delta tables to drive savings on cloud storage.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Delete data using partition boundaries
# MAGIC - Configure downstream incremental reads to safely ignore these deletions
# MAGIC - Use **`VACUUM`** to review files to be deleted and commit deletes
# MAGIC - Union archived data with production tables to recreate a full historic dataset

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by running our setup script.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.3

# COMMAND ----------

# MAGIC %md
# MAGIC Our Delta table is partitioned by two fields. 
# MAGIC 
# MAGIC Our top level partition is the **`topic`** column. 
# MAGIC 
# MAGIC Run the cell to note the 3 partition directories (and the Delta Log directory) that collectively comprise our **`bronze`** table.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Our 2nd level partition was on our **`week_part`** column, which we derived as the year and week of year. There are around 20 directories currently present at this level. 

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze/topic=user_info")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that in our current dataset, we're tracking only a small number of total users in these files.

# COMMAND ----------

total = (spark.table("bronze")
              .filter("topic='user_info'")
              .filter("week_part<='2019-48'")
              .count())
         
print(f"Total: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archiving Data
# MAGIC If a company wishes to maintain an archive of historic records (but only maintain recent records in production tables), cloud-native settings for auto-archiving data can be configured to move data files automatically to lower-cost storage locations.
# MAGIC 
# MAGIC The cell below simulates this process (here using copy instead of move). 
# MAGIC 
# MAGIC Note that because only the data files and partition directories are being relocated, the resultant table will be Parquet by default.
# MAGIC 
# MAGIC **NOTE**: For best performance, directories should have **`OPTIMIZE`** run to condense small files. Because valid and stale data files are stored side-by-side in Delta Lake files, partitions should also have **`VACUUM`** executed prior to moving any Delta Lake data to a pure Parquet table to ensure only valid files are copied.

# COMMAND ----------

archive_path = f"{DA.paths.working_dir}/pii_archive"
source_path = f"{DA.paths.user_db}/bronze/topic=user_info"

files = dbutils.fs.ls(source_path)
[dbutils.fs.cp(f[0], f"{archive_path}/{f[1]}", True) for f in files if f[1][-8:-1] <= '2019-48'];

spark.sql(f"""
CREATE TABLE IF NOT EXISTS user_info_archived
USING parquet
LOCATION '{archive_path}'
""")

spark.sql("MSCK REPAIR TABLE user_info_archived")

display(spark.sql("SELECT COUNT(*) FROM user_info_archived"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the directory structure was maintained as files were copied.

# COMMAND ----------

files = dbutils.fs.ls(archive_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deleting at a Partition Boundary
# MAGIC Here we'll model deleting all **`user_info`** that was received before week 49 of 2019.
# MAGIC 
# MAGIC Note that we are deleting cleanly along partition boundaries. All the data contained in the specified **`week_part`** directories will be removed from our table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM bronze 
# MAGIC WHERE topic = 'user_info'
# MAGIC AND week_part <= '2019-48'

# COMMAND ----------

# MAGIC %md
# MAGIC We can confirm this delete processed successfully by looking at the history. The **`operationMetrics`** column will indicate the number of removed files.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze

# COMMAND ----------

# MAGIC %md
# MAGIC When deleting along partition boundaries, we don't write out new data files; recording the files as removed in the Delta log is sufficient. 
# MAGIC 
# MAGIC However, file deletion will not actually occur until we **`VACUUM`** our table. 
# MAGIC 
# MAGIC Note that all of our week partitions still exist in our **`user_info`** directory and that data files still exist in each week directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{source_path}/week_part=2019-48")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviewing and Committing Deletes
# MAGIC 
# MAGIC By default, the Delta engine will prevent **`VACUUM`** operations with less than 7 days of retention. The cell below overrides this check.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding the **`DRY RUN`** keyword to the end of our **`VACUUM`** statement allows us to preview files to be deleted before they are permanently removed. 
# MAGIC 
# MAGIC Note that at this moment we could still recover our deleted data by running:
# MAGIC 
# MAGIC <strong><code>
# MAGIC RESTORE bronze<br/>
# MAGIC TO VERSION AS OF {version}
# MAGIC </code></strong>

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM bronze RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC Executing the **`VACUUM`** command below permanently deletes these files.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM bronze RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC For safety, it's best to always re-enable our **`retentionDurationCheck`**. In production, you should avoid overriding this check whenever possible (if other operations are acting against files not yet committed to a Delta table and written before the retention threshold, **`VACUUM`** can result in data corruption).

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that empty directories will eventually be cleaned up with **`VACUUM`**, but may not always be deleted as they are emptied of data files.

# COMMAND ----------

try:
    files = dbutils.fs.ls(source_path)
    print("The files NOT YET deleted.")
except Exception as e:
    print("The files WERE deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC As such, querying the **`bronze`** table with the same filters used in our delete statement should yield 0 records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze 
# MAGIC WHERE topic='user_info' AND 
# MAGIC       week_part <= '2019-48'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recreating Full Table History
# MAGIC 
# MAGIC Note that because Parquet using directory partitions as columns in the resulting dataset, the data that was backed up no longer has a **`topic`** field in its schema.
# MAGIC 
# MAGIC The logic below addresses this while calling **`UNION`** on the archived and production datasets to recreate the full history of the **`user_info`** topic.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH full_bronze_user_info AS (
# MAGIC 
# MAGIC   SELECT key, value, partition, offset, timestamp, date, week_part 
# MAGIC   FROM bronze 
# MAGIC   WHERE topic='user_info'
# MAGIC   
# MAGIC   UNION SELECT * FROM user_info_archived) 
# MAGIC   
# MAGIC SELECT COUNT(*) FROM full_bronze_user_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updating Streaming Reads to Ignore Changes
# MAGIC 
# MAGIC The cell below condenses all the code used to perform streaming updates to our **`users`** table.
# MAGIC 
# MAGIC If you try to execute this code right now, you'll raise an exception
# MAGIC > Detected deleted data from streaming source
# MAGIC 
# MAGIC Line 22 of the cell below adds the **`.option("ignoreDeletes", True)`** to the DataStreamReader. This option is all that is necessary to enable streaming processing from Delta tables with partition deletes.

# COMMAND ----------

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

unpacked_df = (spark.readStream
                    .option("ignoreDeletes", True)     # This is new!
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .dropDuplicates()
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            F.col('timestamp').cast("timestamp").alias("updated"),
                            F.to_date('dob','MM/dd/yyyy').alias('dob'),
                            'sex', 'gender','first_name','last_name','address.*', "update_type"))



def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    
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

# COMMAND ----------

query = (unpacked_df.writeStream
                    .foreachBatch(batch_rank_upsert)
                    .outputMode("update")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/batch_rank_upsert")
                    .trigger(once=True)
                    .start())    

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we may see the table version increment as this code completes.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY users

# COMMAND ----------

# MAGIC %md
# MAGIC However, by examining the Delta log file this version, we'll note that the file written out is just indicating the data change, but that no new records were added or modified.

# COMMAND ----------

users_log_path = f"{DA.paths.user_db}/users/_delta_log"
files = dbutils.fs.ls(users_log_path)

max_version = max([file.name for file in files if file.name.endswith(".json")])
display(spark.read.json(f"{users_log_path}/{max_version}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC While we did not modify data in our **`workout`** or **`bpm`** partitions, because these read from the same **`bronze`** table we'll need to also update their DataStreamReader logic to ignore changes.

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
