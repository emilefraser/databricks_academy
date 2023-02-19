# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

# Clean out the global_temp database.
for row in spark.sql("SHOW TABLES IN global_temp").select("tableName").collect():
    table_name = row[0]
    spark.sql(f"DROP TABLE global_temp.{table_name}")

DA.conclude_setup()

