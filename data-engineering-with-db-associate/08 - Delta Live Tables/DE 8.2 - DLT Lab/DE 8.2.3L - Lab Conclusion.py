# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="3c915a05-b7c2-4834-a192-220b12581266"/>
# MAGIC 
# MAGIC 
# MAGIC # Lab: Conclusion
# MAGIC Running the following cell to configure the lab environment:

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.2.3L

# COMMAND ----------

# MAGIC %md <i18n value="c8724cfa-436c-4b5a-9bfb-a8ec0eb8bdb8"/>
# MAGIC 
# MAGIC 
# MAGIC ## Display Results
# MAGIC 
# MAGIC Assuming your pipeline runs successfully, display the contents of the gold table.
# MAGIC 
# MAGIC **NOTE**: Because we specified a value for **Target**, tables are published to the specified database. Without a **Target** specification, we would need to query the table based on its underlying location in DBFS (relative to the **Storage Location**).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.schema_name}.daily_patient_avg

# COMMAND ----------

# MAGIC %md <i18n value="9aa6b45d-5830-4ce0-be11-4b0efb7201c9"/>
# MAGIC 
# MAGIC 
# MAGIC Trigger another file arrival with the following cell. 
# MAGIC 
# MAGIC Feel free to run it a couple more times if desired. 
# MAGIC 
# MAGIC Following this, run the pipeline again and view the results. 
# MAGIC 
# MAGIC Feel free to re-run the cell above to gain an updated view of the **`daily_patient_avg`** table.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="7241d8fc-02f5-4b2a-883c-91691fe4909b"/>
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Ensure that you delete your pipeline from the DLT UI, and run the following cell to clean up the files and tables that were created as part of the lab setup and execution.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md <i18n value="2e81a24b-7230-4565-8179-ad11dc3463ab"/>
# MAGIC 
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lab, you learned to convert an existing data pipeline to a Delta Live Tables SQL pipeline, and deployed that pipeline using the DLT UI.

# COMMAND ----------

# MAGIC %md <i18n value="28dd19b4-019c-4995-b79a-7aba5b36a69f"/>
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Delta Live Tables Documentation</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Delta Live Tables Demo</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
