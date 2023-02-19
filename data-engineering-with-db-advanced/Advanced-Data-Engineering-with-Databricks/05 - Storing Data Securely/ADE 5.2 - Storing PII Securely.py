# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Storing PII Securely
# MAGIC 
# MAGIC Adding a pseudonymized key to incremental workloads is as simple as adding a transformation.
# MAGIC 
# MAGIC In this notebook, we'll examine design patterns for ensuring PII is stored securely and updated accurately. We'll also demonstrate an approach for processing delete requests to make sure these are captured appropriately.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_users.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Apply incremental transformations to store data with pseudonymized keys
# MAGIC - Use windowed ranking to identify the most-recent records in a CDC feed

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.2

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the following cell to create the **`users`** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users
# MAGIC (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.working_dir}/users'

# COMMAND ----------

# MAGIC %md
# MAGIC ## ELT with Pseudonymization
# MAGIC The data in the **`user_info`** topic contains complete row outputs from a Change Data Capture feed.
# MAGIC 
# MAGIC There are three values for **`update_type`** present in the data: **`new`**, **`update`**, and **`delete`**.
# MAGIC 
# MAGIC The **`users`** table will be implemented as a Type 1 table, so only the most recent value matters
# MAGIC 
# MAGIC Run the cell below to visually confirm that both **`new`** and **`update`** records contain all the fields we need for our **`users`** table.

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
        zip: INT>"""

users_df = (spark.table("bronze")
                 .filter("topic = 'user_info'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
                 .filter(F.col("update_type").isin(["new", "update"])))

display(users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication with Windowed Ranking
# MAGIC 
# MAGIC We've previously explored some ways to remove duplicate records:
# MAGIC - Using Delta Lake's **`MERGE`** syntax, we can update or insert records based on keys, matching new records with previously loaded data
# MAGIC - **`dropDuplicates`** will remove exact duplicates within a table or incremental microbatch
# MAGIC 
# MAGIC Now we have multiple records for a given primary key BUT these records are not identical. **`dropDuplicates`** will not work to remove these records, and we'll get an error from our merge statement if we have the same key present multiple times.
# MAGIC 
# MAGIC Below, a third approach for removing duplicates is shown below using the <a href="http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.html?highlight=window#pyspark.sql.Window" target="_blank">PySpark Window class</a>.

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())

ranked_df = (users_df.withColumn("rank", F.rank().over(window))
                     .filter("rank == 1")
                     .drop("rank"))
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC As desired, we get only the newest (**`rank == 1`**) entry for each unique **`user_id`**.
# MAGIC 
# MAGIC Unfortunately, if we try to apply this to a streaming read of our data, we'll learn that
# MAGIC > Non-time-based windows are not supported on streaming DataFrames
# MAGIC 
# MAGIC Uncomment and run the following cell to see this error in action:

# COMMAND ----------

# ranked_df = (spark.readStream
#                   .table("bronze")
#                   .filter("topic = 'user_info'")
#                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
#                   .select("v.*")
#                   .filter(F.col("update_type").isin(["new", "update"]))
#                   .withColumn("rank", F.rank().over(window))
#                   .filter("rank == 1").drop("rank"))

# display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Luckily we have a workaround to avoid this restriction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementing Streaming Ranked De-duplication
# MAGIC 
# MAGIC As we saw previously, when apply **`MERGE`** logic with a Structured Streaming job, we need to use **`foreachBatch`** logic.
# MAGIC 
# MAGIC Recall that while we're inside a streaming microbatch, we interact with our data using batch syntax.
# MAGIC 
# MAGIC This means that if we can apply our ranked **`Window`** logic within our **`foreachBatch`** function, we can avoid the restriction throwing our error.
# MAGIC 
# MAGIC The code below sets up all the incremental logic needed to load in the data in the correct schema from the bronze table. This includes:
# MAGIC - Filter for the **`user_info`** topic
# MAGIC - Dropping identical records within the batch
# MAGIC - Unpack all of the JSON fields from the **`value`** column into the correct schema
# MAGIC - Update field names and types to match the **`users`** table schema
# MAGIC - Use the salted hash function to cast the **`user_id`** to **`alt_id`**

# COMMAND ----------

salt = "BEANS"

unpacked_df = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            F.col("timestamp").cast("timestamp").alias("updated"),
                            F.to_date("dob", "MM/dd/yyyy").alias("dob"), "sex", "gender", "first_name", "last_name", "address.*", "update_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC The updated Window logic is provided below. Note that this is being applied to each **`micro_batch_df`** to result in a local **`ranked_df`** that will be used for merging.
# MAGIC  
# MAGIC For our **`MERGE`** statement, we need to:
# MAGIC - Match entries on our **`alt_id`**
# MAGIC - Update all when matched **if** the new record has is newer than the previous entry
# MAGIC - When not matched, insert all
# MAGIC 
# MAGIC As before, use **`foreachBatch`** to apply merge operations in Structured Streaming.

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())

def batch_rank_upsert(microBatchDF, batchId):
    
    (microBatchDF.filter(F.col("update_type").isin(["new", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
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

# MAGIC %md
# MAGIC Now we can apply this function to our data. 
# MAGIC 
# MAGIC Here, we'll run a trigger-available-now batch to process all records.

# COMMAND ----------

query = (unpacked_df.writeStream
                    .foreachBatch(batch_rank_upsert)
                    .outputMode("update")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/batch_rank_upsert")
                    .trigger(availableNow=True)
                    .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC The **`users`** table should only have 1 record for each unique ID.

# COMMAND ----------

count_a = spark.table("users").count()
count_b = spark.table("users").select("alt_id").distinct().count()
assert count_a == count_b
print("All tests passed.")

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
