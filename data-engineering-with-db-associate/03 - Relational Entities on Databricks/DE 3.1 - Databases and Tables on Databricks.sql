-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="731b610a-2018-40a2-8eae-f6f01ae7a788"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Schemas and Tables on Databricks
-- MAGIC In this demonstration, you will create and explore schemas and tables.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define schemas and tables
-- MAGIC * Describe how the **`LOCATION`** keyword impacts the default storage directory
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="10b2fb72-8534-4903-98a1-26716350dd20"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Lesson Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- MAGIC %md <i18n value="1cbf441b-a62f-4202-af2a-677d37a598b2"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Using Hive Variables
-- MAGIC 
-- MAGIC While not a pattern that is generally recommended in Spark SQL, this notebook will use some Hive variables to substitute in string values derived from the account email of the current user.
-- MAGIC 
-- MAGIC The following cell demonstrates this pattern.

-- COMMAND ----------

SELECT "${da.schema_name}" AS schema_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

-- MAGIC %md <i18n value="014c9f3d-ffd0-48b8-989e-b80b2568d642"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Because you may be working in a shared workspace, this course uses variables derived from your username so the schemas don't conflict with other users. Again, consider this use of Hive variables a hack for our lesson environment rather than a good practice for development.

-- COMMAND ----------

-- MAGIC %md <i18n value="ff022f79-7f38-47ea-809e-537cf00526d0"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Schemas
-- MAGIC Let's start by creating two schemas:
-- MAGIC - One with no **`LOCATION`** specified
-- MAGIC - One with **`LOCATION`** specified

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;
CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md <i18n value="4eff4961-9de3-4d5d-836e-cc48862ef4e6"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Note that the location of the first schema is in the default location under **`dbfs:/user/hive/warehouse/`** and that the schema directory is the name of the schema with the **`.db`** extension

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="58292139-abd2-453b-b327-9ec2ab76dd0a"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note that the location of the second schema is in the directory specified after the **`LOCATION`** keyword.

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="d794ab19-e4e8-4f5c-b784-385ac7c27bc2"/>
-- MAGIC 
-- MAGIC  
-- MAGIC We will create a table in the schema with default location and insert data. 
-- MAGIC 
-- MAGIC Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="17403d69-25b1-44d5-b37f-bab7c091a01b"/>
-- MAGIC 
-- MAGIC  
-- MAGIC We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="71f3a626-a3d4-48a6-8489-6c9cffd021fc"/>
-- MAGIC 
-- MAGIC 
-- MAGIC By default, managed tables in a schema without the location specified will be created in the **`dbfs:/user/hive/warehouse/<schema_name>.db/`** directory.
-- MAGIC 
-- MAGIC We can see that, as expected, the data and metadata for our Delta Table are stored in that location.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hive_root =   f"dbfs:/user/hive/warehouse"
-- MAGIC schema_name = f"{DA.schema_name}_default_location.db"
-- MAGIC table_name =  f"managed_table_in_db_with_default_location"
-- MAGIC 
-- MAGIC tbl_location = f"{hive_root}/{schema_name}/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="ff92a2d3-9bf0-45d0-b78a-c25638ab9479"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="e9c2d161-c157-4d67-8b8d-dbd3d89b6460"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Note the table's directory and its log and data files are deleted. Only the schema directory remains.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location = f"{hive_root}/{schema_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md <i18n value="bd185ea7-cd88-4453-a77a-1babe4633451"/>
-- MAGIC 
-- MAGIC  
-- MAGIC We now create a table in the schema with custom location and insert data. 
-- MAGIC 
-- MAGIC Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.schema_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="68e86e08-9400-428d-9c56-d47439af7dff"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Again, we'll look at the description to find the table location.

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="878787b3-1178-44d1-a775-0bcd6c483184"/>
-- MAGIC 
-- MAGIC  
-- MAGIC As expected, this managed table is created in the path specified with the **`LOCATION`** keyword during schema creation. As such, the data and metadata for the table are persisted in a directory here.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC table_name = f"managed_table_in_db_with_custom_location"
-- MAGIC tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="699d9cda-0276-4d93-bf8c-5e1d370ce113"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Let's drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md <i18n value="c87c1801-0101-4378-9f52-9a8d052a38e1"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Note the table's folder and the log file and data file are deleted.  
-- MAGIC   
-- MAGIC Only the schema location remains

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC 
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md <i18n value="67fd15cf-0ca9-4e76-8806-f24c60d324b1"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Tables
-- MAGIC We will create an external (unmanaged) table from sample data. 
-- MAGIC 
-- MAGIC The data we are going to use are in CSV format. We want to create a Delta table with a **`LOCATION`** provided in the directory of our choice.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${DA.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="367720a7-b738-4782-8f42-571b522c95c2"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="3267ab86-f8fe-40dc-aa52-44aecf8d8fc1"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="b9b3c493-3a09-4fdb-9615-1e8c56824b12"/>
-- MAGIC 
-- MAGIC  
-- MAGIC The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="c456ac65-ab0b-435a-ae00-acbde5048a96"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Clean up
-- MAGIC Drop both schemas.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;
DROP SCHEMA ${da.schema_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fa204d5-12ff-4ede-9fe1-871a346052c4"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
