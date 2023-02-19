# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

if dbgems.is_job():
    # We only create the schema when running under test
    lesson_config.create_schema = True
    DA = DBAcademyHelper(course_config, lesson_config)
    DA.reset_lesson()
    DA.init()

    print("Mocking global temp view")
    spark.sql(f"""USE {DA.schema_name}""")
    spark.sql(f"""CREATE TABLE external_table USING CSV OPTIONS (path='{DA.paths.datasets}/flights/departuredelays.csv', header="true", mode="FAILFAST");""")
    spark.sql(f"""CREATE OR REPLACE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 AS SELECT * FROM external_table WHERE distance > 1000;""")
    
else:
    lesson_config.create_schema = False
    DA = DBAcademyHelper(course_config, lesson_config)
    DA.init()
    
print()

DA.conclude_setup()

