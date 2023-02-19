# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

DA.paths.kafka_events = f"{DA.paths.datasets}/ecommerce/raw/events-kafka"

DA.conclude_setup()

