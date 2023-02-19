# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Continues where 8.1.1 picks up, don't remove assets
lesson_config.name = "dlt_demo_81"

DA = DBAcademyHelper(course_config, lesson_config)
# DA.reset_lesson() # We don't want to reset the environment
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

