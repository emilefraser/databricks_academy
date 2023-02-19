# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config = LessonConfig(name = None,                       # Lesson name feature not needed here.
                             create_schema = False,             # Only create schema if this is not setup
                             create_catalog = False,            # Not a UC course
                             requires_uc = False,               # Not a UC course
                             installing_datasets = True,        # Only instal the datasets if this is not setup
                             enable_streaming_support = False,  # This course doesn't use streaming
                             enable_ml_support = False)         # This course doesn't require ML support

DA = DBAcademyHelper(course_config=course_config, 
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

DA.print_copyrights()

