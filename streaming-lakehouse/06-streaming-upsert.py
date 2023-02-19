# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Upserts with Delta Lake
# MAGIC 
# MAGIC Delta Lake ACID compliance enables many operations to take place in the data lake that would normally require a data warehouse. Delta Lake provides `MERGE` syntax to complete updates, deletes, and inserts as a single transaction.
# MAGIC 
# MAGIC This demo will focus on applying this merge logic to a streaming pipeline.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC * Use `foreachBatch` to apply a custom writer function to a Structured Stream
# MAGIC * Define an upsert statement using Spark SQL
# MAGIC * Define an upsert statement using the Python Delta Lake APIs
# MAGIC 
# MAGIC ## Our Data
# MAGIC 
# MAGIC Both the source and sink tables have already been defined for this demo.
# MAGIC 
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC 
# MAGIC The type of action a row represents is not captured, and each user may have many records present. The field `mrn` serves as the unique identifier.
# MAGIC 
# MAGIC ### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC 
# MAGIC The following code defines some paths, a demo database, and clears out previous runs of the demo. A helper class is also loaded to the variable `Bronze` to allow us to trigger new data arriving in our `bronze` table.

# COMMAND ----------

# MAGIC %run ./Includes/upsert-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Bronze Data
# MAGIC Land a batch of data and display our table.

# COMMAND ----------

Bronze.arrival()
display(spark.sql(f"SELECT * FROM delta.`{bronzePath}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using `foreachBatch` with Structured Streaming
# MAGIC 
# MAGIC The Spark Structured Streaming `foreachBatch` method allows users to define custom logic when writing.
# MAGIC 
# MAGIC The logic applied during `foreachBatch` addresses the present microbatch as if it were a batch (rather than streaming) data. This means that no checkpoint is required for these streams, and that these streams are not stateful.
# MAGIC 
# MAGIC At this time, Delta Lake `merge` logic does not have a native writer in Spark Structured Streaming, so this logic must be implemented by applying a custom function within `foreachBatch`.
# MAGIC 
# MAGIC All functions follow the same basic format:

# COMMAND ----------

def exampleFunction(microBatchDF, batchID):
    microBatchDF #<do something>

# COMMAND ----------

# MAGIC %md
# MAGIC The `microBatchD` argument will be used to capture and manipulate the current microbatch of data as a Spark DataFrame. The `batchID` identifies the microbatch, but can be ignored by your custom writer (but is a necessary argument for `foreachBatch` to work).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Streaming Upsert with Spark SQL
# MAGIC 
# MAGIC The cell below demonstrates using Spark SQL to define a function suitable for performing a streaming upsert. A few things to note:
# MAGIC 1. We can only merge into Delta tables (`silver` is an existing Delta table)
# MAGIC 1. `createOrReplaceTempView` allows us to create a local view to refer to our microbatch data (`stream_batch` is later referenced in our SQL query)
# MAGIC 1. `WHEN MATCHED` is analyzed before our additional conditions (all of the `OR` statements are evaluated together, and applied with a single `AND`)
# MAGIC 
# MAGIC The code below will update all values if a record with the same `mrn` exists and any values have changed, or insert all values if the record has not been seen. Records with no changes will be silently ignored.

# COMMAND ----------

def upsertToDeltaSQL(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("stream_batch")
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver t
        USING stream_batch s
        ON s.mrn = t.mrn
        WHEN MATCHED AND
          s.dob <> t.dob OR
          s.sex <> t.sex OR
          s.gender <> t.gender OR
          s.first_name <> t.first_name OR
          s.last_name <> t.last_name OR
          s.street_address <> t.street_address OR
          s.zip <> t.zip OR
          s.city <> t.city OR
          s.state <> t.state OR
          s.updated <> t.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Define a streaming reading against the bronze Delta table.

# COMMAND ----------

bronzeDF = (spark.readStream
    .format("delta")
    .load(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Use `foreachBatch` to apply our method to each microbatch in our structured stream.

# COMMAND ----------

(bronzeDF.writeStream
    .format("delta")
    .foreachBatch(upsertToDeltaSQL)
    .outputMode("update")
#     .trigger(once=True)
    .trigger(processingTime='2 seconds')
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following query to see the newest updated records first.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Land new data in the `bronze` table and track progress through your streaming query above.

# COMMAND ----------

Bronze.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC Now you should be able to see new data inserted and updated in your table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Stop your stream before continuing.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Streaming Upsert with Python Delta APIs
# MAGIC Delta Lake has a full set of Python APIs that can also be used for performing operations. Again, `foreachBatch` must be used to apply `merge` logic with Structured Streaming.
# MAGIC 
# MAGIC Note that the logic below is identical to our previous SQL query.
# MAGIC 
# MAGIC As before, our upsert is defined in reference to our target `silver` table. As such, the code below begins by loading the `silver` Delta table using the API.
# MAGIC 
# MAGIC We can see the same conditional logic used to specify which matched records should be updated.

# COMMAND ----------

from delta.tables import *

silver_table = DeltaTable.forPath(spark, silverPath)

def upsertToDelta(microBatchDF, batchId):
    (silver_table.alias("t").merge(
        microBatchDF.alias("s"),
        "s.mrn = t.mrn")
        .whenMatchedUpdateAll(
            condition = """s.dob <> t.dob OR
                            s.sex <> t.sex OR
                            s.gender <> t.gender OR
                            s.first_name <> t.first_name OR
                            s.last_name <> t.last_name OR
                            s.street_address <> t.street_address OR
                            s.zip <> t.zip OR
                            s.city <> t.city OR
                            s.state <> t.state OR
                            s.updated <> t.updated""")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC The code below applies our Python `merge` logic with a streaming write.

# COMMAND ----------

(bronzeDF.writeStream
    .format("delta")
    .foreachBatch(upsertToDelta)
    .outputMode("update")
#     .trigger(once=True)
    .trigger(processingTime='2 seconds')
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Land more data in the `bronze` table.

# COMMAND ----------

Bronze.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC Once your batch has processed, run the cell below to see newly updated rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Using `DESCRIBE HISTORY`, we can review the logic applied during our merge statements, as well as check the metrics for number of rows inserted and updated with each write.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure you stop all streams before continuing to the next notebook.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
