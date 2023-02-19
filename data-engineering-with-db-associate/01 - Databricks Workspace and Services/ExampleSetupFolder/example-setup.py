# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_config.create_schema = False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

# TODO
my_name = None

# COMMAND ----------

example_df = spark.range(16)

