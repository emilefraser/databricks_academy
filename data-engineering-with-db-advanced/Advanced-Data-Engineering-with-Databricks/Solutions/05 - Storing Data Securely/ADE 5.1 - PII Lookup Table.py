# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Pseudonymized PII Lookup Table
# MAGIC 
# MAGIC In this lesson we'll create a pseudonymized key for storing potentially sensitive user data.
# MAGIC 
# MAGIC Our approach in this notebook is fairly straightforward; some industries may require more elaborate de-identification to guarantee privacy.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_user_lookup.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe the purpose of "salting" before hashing
# MAGIC - Apply salted hashing to sensitive data for pseudonymization
# MAGIC - Use Auto Loader to process incremental inserts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Load Bronze Data
# MAGIC 
# MAGIC The following cell defines and executes logic to incrementally ingest data into the **`registered_users`** table with Auto Loader.
# MAGIC 
# MAGIC This logic is currently configured to process a single file each time a batch is triggered (currently every 10 seconds).
# MAGIC 
# MAGIC Executing this cell will start an always-on stream that slowly ingests new files as they arrive.

# COMMAND ----------

query = (spark.readStream
              .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.maxFilesPerTrigger", 1)
              .load(DA.paths.raw_user_reg)
              .writeStream
              .option("checkpointLocation", f"{DA.paths.checkpoints}/registered_users")
              .trigger(processingTime="10 seconds")
              .table("registered_users"))

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md 
# MAGIC Before moving on with this lesson, we need to:
# MAGIC 1. Stop the existing stream
# MAGIC 2. Drop the table we created
# MAGIC 3. Clear the checkpoint directory

# COMMAND ----------

query.stop()
query.awaitTermination()
spark.sql("DROP TABLE IF EXISTS registered_users")
dbutils.fs.rm(f"{DA.paths.checkpoints}/registered_users", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Use the cell below to refactor the above query into a function that processes new files as a single incremental triggered batch.
# MAGIC 
# MAGIC To do this:
# MAGIC * Remove the option limiting the amount of files processed per trigger (this is ignored when executing a batch anyway)
# MAGIC * Change the trigger type to "availableNow"
# MAGIC * Make sure to add **`.awaitTermination()`** to the end of your query to block execution of the next cell until the batch has completed

# COMMAND ----------

# ANSWER
def ingest_user_reg():
    query = (spark.readStream
                  .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .load(DA.paths.raw_user_reg)
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/registered_users")
                  .trigger(availableNow=True)
                  .option("path", f"{DA.paths.user_db}/registered_users")
                  .table("registered_users"))
    
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Use your function below.
# MAGIC 
# MAGIC **NOTE**: This triggered batch will automatically cause our always-on stream to fail because the same checkpoint is used; default behavior will allow the newer query to succeed and error the older query.

# COMMAND ----------

ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC Another custom class was initialized in the setup script to land a new batch of data in our source directory. 
# MAGIC 
# MAGIC Execute the following cell and note the difference in the total number of rows in our tables.

# COMMAND ----------

DA.user_reg_stream.load()

ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Salt to Natural Key
# MAGIC We'll start by defining a salt, here in plain text. We'll combine this salt with our natural key, **`user_id`**, before applying a hash function to generate a pseudonymized key.
# MAGIC 
# MAGIC Salting before hashing is very important as it makes dictionary attacks to reverse the hash much more expensive.  To demonstrate, try reversing the following SHA-256 hash of `Secrets123` by searching it's hash using Google: `FCF730B6D95236ECD3C9FC2D92D7B6B2BB061514961AEC041D6C7A7192F592E4` bringing you to this [link](https://hashtoolkit.com/decrypt-sha256-hash/fcf730b6d95236ecd3c9fc2d92d7b6b2bb061514961aec041d6c7a7192f592e4).  Next, try hashing `Secrets123:BEANS` [here](https://passwordsgenerator.net/sha256-hash-generator/) and perform a similar search.  Notice how adding the salt `BEANS` improved the security.
# MAGIC 
# MAGIC For greater security, we could upload the salt as a secret using the Databricks <a href="https://docs.databricks.com/security/secrets/secrets.html" target="_blank">Secrets</a> utility.

# COMMAND ----------

salt = 'BEANS'
spark.conf.set("da.salt", salt)

# COMMAND ----------

# # If using the Databricks secrets store, here's how you'd read it...
# salt = dbutils.secrets.get(scope="DA-ADE3.03", key="salt")
# salt

# COMMAND ----------

# MAGIC %md
# MAGIC Preview what your new key will look like.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, sha2(concat(user_id,"${da.salt}"), 256) AS alt_id
# MAGIC FROM registered_users

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a SQL UDF
# MAGIC 
# MAGIC Create a SQL user-defined function to register this logic to the current database under the name **`salted_hash`**. 
# MAGIC 
# MAGIC This will allow this logic to be called by any user with appropriate permissions on this function. 
# MAGIC 
# MAGIC Make sure your UDF accepts one parameter: a **`String`** and it should return a **`STRING`**.  You can access the configured salt value by using the expression `"${da.salt}"`.
# MAGIC 
# MAGIC For more information, see the <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html" target="_blank">CREATE FUNCTION</a> method from the SQL UDFs docs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE OR REPLACE FUNCTION salted_hash (id STRING) RETURNS STRING
# MAGIC RETURN sha2(concat(id,"${da.salt}"), 256)

# COMMAND ----------

# MAGIC %md
# MAGIC If your SQL UDF is defined correctly, the assert statement below should run without error.

# COMMAND ----------

# Check your work
set_a = spark.sql(f"SELECT sha2(concat('USER123', '{salt}'), 256) alt_id").collect()
set_b = spark.sql("SELECT salted_hash('USER123') alt_id").collect()
assert set_a == set_b, "The 'salted_hash' function is returning the wrong result."
print("All tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that it is theoretically possible to link the original key and pseudo-ID if the hash function and the salt are known.
# MAGIC 
# MAGIC Here, we use this method to add a layer of obfuscation; in production, you may wish to have a much more sophisticated hashing method.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Target Table
# MAGIC The logic below creates the **`user_lookup`** table.
# MAGIC 
# MAGIC Here we're just creating our **`user_lookup`** table. In the next notebook, we'll use this pseudo-ID as the sole link to user PII.
# MAGIC 
# MAGIC By controlling access to the link between our **`alt_id`** and other natural keys, we'll be able to prevent linking PII to other user data throughout our system.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS user_lookup (alt_id string, device_id long, mac_address string, user_id long)
# MAGIC USING DELTA 
# MAGIC LOCATION '${da.paths.working_dir}/user_lookup'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Function for Processing Incremental Batches
# MAGIC 
# MAGIC The cell below includes the setting for the correct checkpoint path.
# MAGIC 
# MAGIC Define a function to apply the SQL UDF registered above to create your **`alt_id`** to the **`user_id`** from the **`registered_users`** table.
# MAGIC 
# MAGIC Make sure you include all the necessary columns for the target **`user_lookup`** table. Configure your logic to execute as a triggered incremental batch.

# COMMAND ----------

# ANSWER
def load_user_lookup():
    query = (spark.readStream
                  .table("registered_users")
                  .selectExpr("salted_hash(user_id) AS alt_id", "device_id", "mac_address", "user_id")
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/user_lookup")
                  .trigger(availableNow=True)
                  .table("user_lookup"))
    
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Use your method below and display the results.

# COMMAND ----------

load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC Process another batch of data below to confirm that the incremental processing is working through the entire pipeline.

# COMMAND ----------

DA.user_reg_stream.load()

ingest_user_reg()
load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC The code below ingests all the remaining records to put 100 total users in the **`user_lookup`** table.

# COMMAND ----------

DA.user_reg_stream.load(continuous=True)

ingest_user_reg()
load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll apply this same hashing method to process and store PII data in the next lesson.

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
