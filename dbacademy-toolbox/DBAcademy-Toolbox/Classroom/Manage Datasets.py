# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage Datasets
# MAGIC The purpose of this notebook is to download and install datasets used by each course
# MAGIC 
# MAGIC Known usages include:
# MAGIC * Data Analysis with Databricks

# COMMAND ----------

# MAGIC %md ## Initialize Notebook
# MAGIC Run the following two cells to initialize this notebook.
# MAGIC 
# MAGIC Once initialized, select your options from the widgets above.

# COMMAND ----------

# MAGIC %run ../Includes/Common

# COMMAND ----------

init_course()

# COMMAND ----------

# MAGIC %md ## Review Your Selections
# MAGIC Run the following cell to review your selections:

# COMMAND ----------

target_dir = f"dbfs:/mnt/dbacademy-datasets/{get_dataset_repo()}/{get_data_source_version()}"

print(f"Course Code:      {get_course_code()}")
print(f"Course Name:      {get_course_name()}")
print(f"Dataset Repo:     {get_data_source_uri()}")
print(f"Target Directory: {target_dir}")

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Delete Existing Dataset
# MAGIC This should only be executed to "repair" a datsets.

# COMMAND ----------

dbutils.fs.rm(target_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Install Dataset

# COMMAND ----------

files = dbutils.fs.ls(f"{get_data_source_uri()}")

for i, file in enumerate(files):
    start = int(time.time())
    print(f"Installing dataset {i+1}/{len(files)}, {file.name}", end="...")
    dbutils.fs.cp(file.path, f"{target_dir}/{file.name}", True)
    print(f"({int(time.time())-start} seconds)")
    
display(dbutils.fs.ls(target_dir))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
