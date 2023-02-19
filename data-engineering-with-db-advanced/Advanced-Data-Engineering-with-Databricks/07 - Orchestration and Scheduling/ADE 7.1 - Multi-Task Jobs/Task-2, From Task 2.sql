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

INSERT INTO task_2 VALUES ("From Task #2", current_timestamp());

-- COMMAND ----------

INSERT INTO task_3 VALUES ("From Task #2", current_timestamp());

-- COMMAND ----------

SELECT COUNT(*) FROM task_2

-- COMMAND ----------

SELECT * FROM task_2

-- COMMAND ----------

SELECT * FROM task_3

