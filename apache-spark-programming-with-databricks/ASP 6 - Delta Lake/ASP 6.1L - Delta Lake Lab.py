# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Delta Lake Lab
# MAGIC ##### Tasks
# MAGIC 1. Write sales data to Delta
# MAGIC 1. Modify sales data to show item count instead of item array
# MAGIC 1. Rewrite sales data to same Delta path
# MAGIC 1. Create table and view version history
# MAGIC 1. Time travel to read previous version

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

sales_df = spark.read.parquet(f"{DA.paths.datasets}/ecommerce/sales/sales.parquet")
delta_sales_path = f"{DA.paths.working_dir}/delta-sales"

# COMMAND ----------

# MAGIC %md ### 1. Write sales data to Delta
# MAGIC Write **`sales_df`** to **`delta_sales_path`**

# COMMAND ----------

# TODO
sales_df.FILL_IN

# COMMAND ----------

# MAGIC %md **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert len(dbutils.fs.ls(delta_sales_path)) > 0

# COMMAND ----------

# MAGIC %md ### 2. Modify sales data to show item count instead of item array
# MAGIC Replace values in the **`items`** column with an integer value of the items array size.
# MAGIC Assign the resulting DataFrame to **`updated_sales_df`**.

# COMMAND ----------

# TODO
updated_sales_df = FILL_IN
display(updated_sales_df)

# COMMAND ----------

# MAGIC %md **2.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import IntegerType

assert updated_sales_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 3. Rewrite sales data to same Delta path
# MAGIC Write **`updated_sales_df`** to the same Delta location **`delta_sales_path`**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> This will fail without an option to overwrite the schema.

# COMMAND ----------

# TODO
updated_sales_df.FILL_IN

# COMMAND ----------

# MAGIC %md **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert spark.read.format("delta").load(delta_sales_path).schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 4. Create table and view version history
# MAGIC Run SQL queries by writing SQL inside of `spark.sql()` to perform the following steps.
# MAGIC - Drop table **`sales_delta`** if it exists
# MAGIC - Create **`sales_delta`** table using the **`delta_sales_path`** location
# MAGIC - List version history for the **`sales_delta`** table
# MAGIC 
# MAGIC An example of a SQL query inside of `spark.sql()` would be something like ```spark.sql("SELECT * FROM sales_data")```

# COMMAND ----------

# TODO

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md **4.1: CHECK YOUR WORK**

# COMMAND ----------

sales_delta_df = spark.sql("SELECT * FROM sales_delta")
assert sales_delta_df.count() == 210370
assert sales_delta_df.schema[6].dataType == IntegerType()
print("All test pass")

# COMMAND ----------

# MAGIC %md ### 5. Time travel to read previous version
# MAGIC Read delta table at **`delta_sales_path`** at version 0.
# MAGIC Assign the resulting DataFrame to **`old_sales_df`**.

# COMMAND ----------

# TODO
old_sales_df = FILL_IN
display(old_sales_df)

# COMMAND ----------

# MAGIC %md **5.1: CHECK YOUR WORK**

# COMMAND ----------

assert old_sales_df.select(size(col("items"))).first()[0] == 1
print("All test pass")

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
