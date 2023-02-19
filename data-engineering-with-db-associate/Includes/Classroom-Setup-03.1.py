# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Overriding the instance defined in _common
lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

# COMMAND ----------

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

