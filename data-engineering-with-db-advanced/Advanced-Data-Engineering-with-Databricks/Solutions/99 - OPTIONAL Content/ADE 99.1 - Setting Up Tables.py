# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Setting Up Tables
# MAGIC Managing database and table metadata, locations, and configurations at the beginning of project can help to increase data security, discoverability, and performance.
# MAGIC 
# MAGIC Databricks allows you to store additional information about your databases and tables in a number of locations. This notebook reviews the basics of table and database declaration while introducing various options.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Set database locations
# MAGIC - Specify database comments
# MAGIC - Set table locations
# MAGIC - Specify table comments
# MAGIC - Specify column comments
# MAGIC - Use table properties for custom tagging
# MAGIC - Explore table metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC As we saw in the previous lesson, we need to execute the following script to configure our learning environment.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important Database Options
# MAGIC 
# MAGIC The single most important option when creating a new database is the **`LOCATION`**. Because all managed tables will have their data files stored in this location, modifying the location of a database after initial declaration can require migration of many data files. Using an improper location can potentially store data in an unsecured location and lead to data exfiltration or deletion.
# MAGIC 
# MAGIC The database **`COMMENT`** option allows an arbitrary description to be provided for the database. This can be useful for both discovery and auditing purposes.
# MAGIC 
# MAGIC The **`DBPROPERTIES`** option allows user-defined keys to be specified for the database. This can be useful for creating tags that will be used in auditing. Note that this field may also contain options used elsewhere in Databricks to govern default behavior for the database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Database with Options
# MAGIC 
# MAGIC The following cell demonstrates the syntax for creating a database while:
# MAGIC 1. Setting a database comment
# MAGIC 1. Specifying a database location
# MAGIC 1. Adding an arbitrary key-value pair as a database property
# MAGIC 
# MAGIC An arbitrary directory on the DBFS root is being used for the location; in any stage of development or production, it is best practice to create databases in secure cloud object storage with credentials locked down to appropriate teams within the organization.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${da.db_name}
# MAGIC COMMENT "This is a test database"
# MAGIC LOCATION "${da.paths.user_db}"
# MAGIC WITH DBPROPERTIES (contains_pii = true)

# COMMAND ----------

# MAGIC %md
# MAGIC All of the comments and properties set during database declaration can be reviewed using **`DESCRIBE DATABASE EXTENDED`**.
# MAGIC 
# MAGIC This information can aid in data discovery, auditing, and governance. Having proactive rules about how databases will be created and tagged can help prevent accidental data exfiltration, redundancies, and deletions.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED ${da.db_name}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important Table Options
# MAGIC 
# MAGIC The **`LOCATION`** keyword also plays a pivotal role when declaring tables, as it determines whether a table is **managed** or **external**. Note that explicitly providing a location will always result in an external table, even if this location maps directly to the directory that would be used by default.
# MAGIC 
# MAGIC Tables also have the **`COMMENT`** option to provide a table description.
# MAGIC 
# MAGIC Tables have the **`TBLPROPERTIES`** option that can also contain user-defined tags. All Delta Lake tables will have some default options stored to this field, and many customizations for Delta behavior will be reflected here.
# MAGIC 
# MAGIC Users can also define a **`COMMENT`** for an individual column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Table with Options
# MAGIC The following cell demonstrates creating a **managed** Delta Lake table while:
# MAGIC 1. Setting a column comment
# MAGIC 1. Setting a table comment
# MAGIC 1. Adding an arbitrary key-value pair as a table property

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${da.db_name}.pii_test
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %md
# MAGIC Much like the command for reviewing database metadata settings, **`DESCRIBE EXTENDED`** allows users to see all of the comments and properties for a given table.
# MAGIC 
# MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED ${da.db_name}.pii_test

# COMMAND ----------

# MAGIC %md
# MAGIC Below the code from above is replicated with the addition of specifying a location, creating an **external** table.
# MAGIC 
# MAGIC **NOTE**: The only thing that differentiates managed and external tables is this location setting. Performance of managed and external tables should be equivalent with regards to latency, but the results of SQL DDL statements on these tables differ drastically.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${da.db_name}.pii_test_2
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "${da.paths.working_dir}/pii_test_2"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, the only differences in the extended description of the table have to do with the table location and type.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED ${da.db_name}.pii_test_2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Table Metadata
# MAGIC 
# MAGIC Assuming that rules are followed appropriately when creating databases and tables, comments, table properties, and other metadata can be interacted with programmatically for discovering datasets for governance and auditing purposes.
# MAGIC 
# MAGIC The Python code below demonstrates parsing the table properties field, filtering those options that are specifically geared toward controlling Delta Lake behavior. In this case, logic could be written to further parse these properties to identify all tables in a database that contain PII.

# COMMAND ----------

def parse_table_keys(database, table=None):
    table_keys = {}
    if table:
        query = f"SHOW TABLES IN {DA.db_name} LIKE '{table}'"
    else:
        query = f"SHOW TABLES IN {DA.db_name}"
    for table_item in spark.sql(query).collect():
        table_name = table_item[1]
        key_values = spark.sql(f"DESCRIBE EXTENDED {DA.db_name}.{table_name}").filter("col_name = 'Table Properties'").collect()[0][1][1:-1].split(",")
        table_keys[table_name] = [kv for kv in key_values if not kv.startswith("delta.")]
    return table_keys

parse_table_keys(DA.db_name)   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code Challenge
# MAGIC 
# MAGIC Use the following cell to:
# MAGIC 1. Create a new **managed** table
# MAGIC 1. Using the database and table name provided
# MAGIC 1. Define 4 columns with <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html" target="_blank">any valid data type</a>
# MAGIC 1. Add a table comment
# MAGIC 1. Use the **`TBLPROPERTIES`** option to set the key-value pair **`'contains_pii' = False`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE TABLE IF NOT EXISTS ${da.db_name}.challenge
# MAGIC (col1 STRING, col2 DATE, col3 DOUBLE, col4 INT)
# MAGIC COMMENT "This is a table"
# MAGIC TBLPROPERTIES ('contains_pii' = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the checks below to confirm this:

# COMMAND ----------

assert len(spark.sql(f"SHOW TABLES IN {DA.db_name} LIKE 'challenge'").collect()) > 0, f"Table 'challenge' not in {da.db_name}"
assert parse_table_keys(DA.db_name, 'challenge') == {'challenge': ['contains_pii=false']}, "PII flag not set correctly"
assert len(spark.table(f"{DA.db_name}.challenge").columns) == 4, "Table does not have 4 columns"
assert [x.tableType for x in spark.catalog.listTables(DA.db_name) if x.name=="challenge"] == ["MANAGED"], "Table is not managed"
assert spark.sql(f"DESCRIBE EXTENDED {DA.db_name}.challenge").filter("col_name = 'Comment'").collect() != [], "Table comment not set"
print("All tests passed.")

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
