# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_demo_tmp_vw(self):
    print("\nCreating the temp view \"demo_tmp_vw\"")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW demo_tmp_vw(name, value) AS VALUES
        ("Yi", 1),
        ("Ali", 2),
        ("Selina", 3)
        """)

# COMMAND ----------

# Overriding the instance defined in _common
lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = True,
                             enable_ml_support = False)

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

# Create demo table
DA.create_demo_tmp_vw()                  

DA.conclude_setup()

