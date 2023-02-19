-- Databricks notebook source
-- MAGIC %python
-- MAGIC import re
-- MAGIC username = spark.sql("SELECT current_user()").first()[0]
-- MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
-- MAGIC database = f"dbacademy_{clean_username}_adewd_jobs"
-- MAGIC spark.conf.set("da.user_db", f"dbacademy_{clean_username}_adewd_jobs")

-- COMMAND ----------

USE ${da.user_db}

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_4
(key STRING, run_time TIMESTAMP);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use the widgets API to retreive job parameters
-- MAGIC value = dbutils.widgets.get("name")
-- MAGIC # Inject into context for use in the following SQL statements
-- MAGIC spark.conf.set("name", value) 

-- COMMAND ----------

INSERT INTO task_4 VALUES ('From ${name}', current_timestamp())

-- COMMAND ----------

SELECT COUNT(*) FROM task_4

-- COMMAND ----------

SELECT * FROM task_4

