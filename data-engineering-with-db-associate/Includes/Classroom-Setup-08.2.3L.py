# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.name = "dlt_lab_82"

DA = DBAcademyHelper(course_config, lesson_config)
# DA.reset_lesson() # We don't want to reset the environment
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

