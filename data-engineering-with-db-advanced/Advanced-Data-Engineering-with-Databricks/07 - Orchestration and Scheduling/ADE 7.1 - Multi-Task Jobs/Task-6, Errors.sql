-- Databricks notebook source
-- MAGIC %python
-- MAGIC import re
-- MAGIC username = spark.sql("SELECT current_user()").first()[0]
-- MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
-- MAGIC spark.conf.set("da.user_db", f"dbacademy_{clean_username}_adewd_jobs")

-- COMMAND ----------

USE ${da.user_db}

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_6
(run_time TIMESTAMP);

-- COMMAND ----------

INSERT INTO task_6 VALUES (current_timestamp())

-- COMMAND ----------

-- TODO
-- Expected failure... after configuring and running the multi-task job
-- with the error, come back, fix the error, and then rerun the job.
SELECT COUNT(potato) FROM task_6

-- COMMAND ----------

SELECT * FROM task_6

