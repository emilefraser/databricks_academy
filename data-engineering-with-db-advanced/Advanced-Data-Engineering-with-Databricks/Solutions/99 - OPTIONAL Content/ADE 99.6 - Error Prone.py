# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Parsing Errors
# MAGIC 
# MAGIC This code is going to throw several errors. Click on **`Run All`** above.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.6

# COMMAND ----------

# ANSWER
x = 1
x * 7

# COMMAND ----------

# MAGIC %md
# MAGIC Note that **`Run All`** execution mimics scheduled job execution; the **`Command skipped`** output we see below is the same we'll see in a job result.

# COMMAND ----------

# ANSWER
y = 99.52
y // 1

# COMMAND ----------

# MAGIC %md 
# MAGIC The above is what we see when have Python errors

# COMMAND ----------

# ANSWER
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at a Spark error.
# MAGIC 
# MAGIC While running multiple commands in a single cell, it can sometimes to be difficult to parse where an error is coming from.

# COMMAND ----------

# ANSWER
df = (spark.read
    .format("csv")
    .option("header", True)
    .schema("date DATE, temp INTEGER")
    .load("/databricks-datasets/weather/low_temps"))

df.createOrReplaceTempView("low_temps")

df.join(df, "date").groupBy("date").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Sometimes an error isn't an error, but doesn't achieve what was intended.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT dayofmonth(date) FROM low_temps

# COMMAND ----------

# MAGIC %md
# MAGIC Use the below cell to figure out how to fix the code above.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Column names cause common errors when trying to save tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE TABLE test_table
# MAGIC AS (
# MAGIC   SELECT dayofmonth(date) % 3 three_day_cycle FROM low_temps
# MAGIC )

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
