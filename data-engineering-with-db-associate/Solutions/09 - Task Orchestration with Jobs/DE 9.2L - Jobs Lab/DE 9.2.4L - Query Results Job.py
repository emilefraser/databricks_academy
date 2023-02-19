# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-09.2.4L

# COMMAND ----------

# MAGIC %md <i18n value="1372d675-796a-4bbb-9d83-356d3b1b297e"/>
# MAGIC 
# MAGIC 
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC Run the following cell to enumerate the output of your storage location:

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="9a19b4f8-479b-4dfb-80a8-05e08cfd9eb0"/>
# MAGIC 
# MAGIC 
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="da333edd-ee6d-4892-9da0-29f38d96e14f"/>
# MAGIC 
# MAGIC 
# MAGIC These event logs are stored as a Delta table. 
# MAGIC 
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.working_dir}/storage/system/events`

# COMMAND ----------

# MAGIC %md <i18n value="303e482b-e3cd-48eb-b9be-5357e07803aa"/>
# MAGIC 
# MAGIC 
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/tables")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="6c5ffd14-74db-4ec2-8764-526da08840ab"/>
# MAGIC 
# MAGIC 
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

