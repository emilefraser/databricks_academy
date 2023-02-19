# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.name = "jobs_lab_92"
lesson_config.create_schema = False

DA = DBAcademyHelper(course_config, lesson_config)
# DA.reset_lesson() # We don't want to reset the environment
DA.init()
DA.conclude_setup()

