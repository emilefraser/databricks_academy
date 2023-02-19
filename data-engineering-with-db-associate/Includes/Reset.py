# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.create_schema = False
lesson_config.installing_datasets = False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_learning_environment()

