# Databricks notebook source
# MAGIC %md <i18n value="46a8383a-807d-42a9-9bdc-09a34e0b7bd0"/>
# MAGIC 
# MAGIC 
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This notebook explores the execution results of a DLT pipeline.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="4b109d6f-b0d4-4ded-ac54-a12f722599a9"/>
# MAGIC 
# MAGIC 
# MAGIC The **`system`** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="fd83d9bb-db62-456a-8a51-33e8680c0d47"/>
# MAGIC 
# MAGIC 
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md <i18n value="b0c12205-fc10-4a63-a73e-d5cded65ef51"/>
# MAGIC 
# MAGIC 
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="e5072a3f-3a1f-4e4d-89fa-bda3926cb6ba"/>
# MAGIC 
# MAGIC 
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.schema_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md <i18n value="69333106-f421-45f0-a846-1c3d4fc8ddcb"/>
# MAGIC 
# MAGIC  
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

