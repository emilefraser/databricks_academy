# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="1.3"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def create_new_records():
    DA.new_records_tbl = "new_records"
    spark.conf.set("da.new_records_tbl", DA.new_records_tbl)
    spark.sql("CREATE OR REPLACE TABLE ${da.new_records_tbl} (id INT, name STRING, value DOUBLE)")
    spark.sql("""INSERT INTO ${da.new_records_tbl} VALUES 
        (7, "Viktor", 7.4),
        (8, "Hiro", 8.2),
        (9, "Shana", 9.9)""")
    spark.sql("OPTIMIZE new_records")

def create_updates():
    DA.updates_tbl = "updates" 
    spark.conf.set("da.updates_tbl", DA.updates_tbl)
    spark.sql("CREATE OR REPLACE TABLE ${da.updates_tbl} (id INT, name STRING, value DOUBLE, type STRING)")
    spark.sql("""INSERT INTO ${da.updates_tbl} VALUES 
        (2, "Omar", 15.2, "update"),
        (3, "", null, "delete"),
        (10, "Blue", 7.7, "insert"),
        (11, "Diya", 8.8, "update")""")
      

# COMMAND ----------

DA.cleanup()
DA.init()

create_new_records()
create_updates()

DA.conclude_setup()

