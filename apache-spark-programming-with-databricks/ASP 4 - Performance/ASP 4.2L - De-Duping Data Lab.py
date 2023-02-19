# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # De-Duping Data Lab
# MAGIC 
# MAGIC In this exercise, we're doing ETL on a file we've received from a customer. That file contains data about people, including:
# MAGIC 
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC 
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC 
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL").
# MAGIC * The Social Security numbers aren't consistent either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC 
# MAGIC If all of the name fields match -- if you disregard character case -- then the birth dates and salaries are guaranteed to match as well,
# MAGIC and the Social Security Numbers *would* match if they were somehow put in the same format.
# MAGIC 
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. For example, if you write the first name column in all lowercase, you haven't met this requirement.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The initial dataset contains 103,000 records.
# MAGIC The de-duplicated result has 100,000 records.
# MAGIC 
# MAGIC Next, write the results in **Delta** format as a **single data file** to the directory given by the variable **delta_dest_dir**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Remember the relationship between the number of partitions in a DataFrame and the number of files written.
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html" target="_blank">DataFrameReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html" target="_blank">DataFrameWriter</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC It's helpful to look at the file first, so you can check the format with **`dbutils.fs.head()`**.

# COMMAND ----------

dbutils.fs.head(f"{DA.paths.datasets}/people/people-with-dups.txt")

# COMMAND ----------

# TODO

source_file = f"{DA.paths.datasets}/people/people-with-dups.txt"
delta_dest_dir = f"{DA.paths.working_dir}/people"

# In case it already exists
dbutils.fs.rm(delta_dest_dir, True)

# Complete your work here...


# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(delta_dest_dir)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files == 1, "Expected 1 data file written"

verify_record_count = spark.read.format("delta").load(delta_dest_dir).count()
assert verify_record_count == 100000, "Expected 100000 records in final result"

del verify_files, verify_delta_format, verify_num_data_files, verify_record_count
print("All test pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up classroom
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
