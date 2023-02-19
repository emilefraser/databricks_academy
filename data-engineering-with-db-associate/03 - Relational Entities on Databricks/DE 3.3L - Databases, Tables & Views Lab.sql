-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="047624c1-1764-4d00-8f75-2640a0b9ad8e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Databases, Tables, and Views Lab
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create and explore interactions between various relational entities, including:
-- MAGIC   - Databases
-- MAGIC   - Tables (managed and external)
-- MAGIC   - Views (views, temp views, and global temp views)
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databases and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="702fc20d-b0bf-4138-9045-49571c496cc0"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### Getting Started
-- MAGIC 
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- MAGIC %md <i18n value="306e4a60-45cf-40af-850f-4339700000b8"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celsius. The schema for the table:
-- MAGIC 
-- MAGIC |ColumnName  | DataType| Description|
-- MAGIC |------------|---------|------------|
-- MAGIC |NAME        |string   | Station name |
-- MAGIC |STATION     |string   | Unique ID |
-- MAGIC |LATITUDE    |float    | Latitude |
-- MAGIC |LONGITUDE   |float    | Longitude |
-- MAGIC |ELEVATION   |float    | Elevation |
-- MAGIC |DATE        |date     | YYYY-MM-DD |
-- MAGIC |UNIT        |string   | Temperature units |
-- MAGIC |TAVG        |float    | Average temperature |
-- MAGIC 
-- MAGIC This data is stored in the Parquet format; preview the data with the query below.

-- COMMAND ----------

SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="9b640cc4-561c-4f2e-8db4-806496e0300f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Database
-- MAGIC 
-- MAGIC Create a database in the default location using the **`da.schema_name`** variable defined in setup script.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="eb27d1be-83d9-44d6-a3c5-a330d58f4d1b"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.schema_name}'").count() == 1, "Database not present"

-- COMMAND ----------

-- MAGIC %md <i18n value="4007b86a-f1c2-4431-9605-278256a18502"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Change to Your New Database
-- MAGIC 
-- MAGIC **`USE`** your newly created database.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="a1799160-4921-48a9-84fd-d6b30eda2294"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.schema_name, "Not using the correct database"

-- COMMAND ----------

-- MAGIC %md <i18n value="29616225-cd27-4d1f-abf3-70257760ba80"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Managed Table
-- MAGIC Use a CTAS statement to create a managed table named **`weather_managed`**.

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="77e8b20b-0627-49a9-9d59-cf7e02225a64"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="155e14f1-65cf-40be-9d01-68b3775c2381"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create an External Table
-- MAGIC 
-- MAGIC Recall that an external table differs from a managed table through specification of a location. Create an external table called **`weather_external`** below.

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

-- MAGIC %md <i18n value="b7853935-465f-406f-8742-46a2a00ad3b5"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_external"), "Table named `weather_external` does not exist"
-- MAGIC assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="3992398d-0f77-4fc2-8b9e-2f4064f10480"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Examine Table Details
-- MAGIC Use the SQL command **`DESCRIBE EXTENDED table_name`** to examine the two weather tables.

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md <i18n value="6996903c-737b-4b88-8d51-3e0a01b347be"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following helper code to extract and compare the table locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC 
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC 
-- MAGIC     {managedTablePath}
-- MAGIC 
-- MAGIC The weather_external table is saved at:
-- MAGIC 
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md <i18n value="63b73ebc-ccd0-460b-87ae-e09addada714"/>
-- MAGIC 
-- MAGIC 
-- MAGIC List the contents of these directories to confirm that data exists in both locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="09f43b5b-e050-4e58-9769-e2e01829ddbc"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### Check Directory Contents after Dropping Database and All Tables
-- MAGIC The **`CASCADE`** keyword will accomplish this.

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.schema_name}

-- COMMAND ----------

-- MAGIC %md <i18n value="e7babacd-bed8-47d7-ad52-7cc644e4f06a"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.schema_name}'").count() == 0, "Database present"

-- COMMAND ----------

-- MAGIC %md <i18n value="7d7053b8-e1b9-421a-9687-b205feadbf68"/>
-- MAGIC 
-- MAGIC 
-- MAGIC With the database dropped, the files will have been deleted as well.
-- MAGIC 
-- MAGIC Uncomment and run the following cell, which will throw a **`FileNotFoundException`** as your confirmation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # files = dbutils.fs.ls(managedTablePath)
-- MAGIC # display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(DA.paths.working_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="5b66fbc5-c641-40e1-90a9-d69271bc0e8b"/>
-- MAGIC 
-- MAGIC 
-- MAGIC **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
-- MAGIC 
-- MAGIC Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

-- MAGIC %md <i18n value="1928eede-5218-47df-affe-b6a7654524ab"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Database with a Specified Path
-- MAGIC 
-- MAGIC Assuming you dropped your database in the last step, you can use the same **`database`** name.

-- COMMAND ----------

CREATE DATABASE ${da.schema_name} LOCATION '${da.paths.working_dir}/${da.schema_name}';
USE ${da.schema_name};

-- COMMAND ----------

-- MAGIC %md <i18n value="5b5dbf00-f9ee-4bc5-964e-22376e09be79"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Recreate your **`weather_managed`** table in this new database and print out the location of this table.

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md <i18n value="91684fed-3851-4979-be1d-ba8af8cfe314"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md <i18n value="7ddc86a3-fbc4-4373-aacb-2f510c1eb708"/>
-- MAGIC 
-- MAGIC 
-- MAGIC While here we're using the **`working_dir`** directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

-- COMMAND ----------

-- MAGIC %md <i18n value="07f87efe-13e4-48c3-84a7-576828359464"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Views and their Scoping
-- MAGIC 
-- MAGIC In this section, use the provided **`AS`** clause to register:
-- MAGIC - a view named **`celsius`**
-- MAGIC - a temporary view named **`celsius_temp`**
-- MAGIC - a global temp view named **`celsius_global`**
-- MAGIC 
-- MAGIC Start by creating the first view in the code cell below.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="14937501-7f2a-469d-b47f-db0c656d8da3"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius"), "Table named `celsius` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

-- MAGIC %md <i18n value="1465024c-c6c9-4043-9bfa-54a5e9ad8b04"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Now create a temporary view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="5593edb6-0a98-4b8f-af24-4c9caa65dac2"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

-- MAGIC %md <i18n value="a89de892-1615-4f60-a8d2-0da7aebfda14"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Now register a global temp view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md <i18n value="4cb8cfe6-f058-48df-8807-1338907261f7"/>
-- MAGIC 
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

-- MAGIC %md <i18n value="dde3cbe9-3ed4-497a-b778-ca6268b57973"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md <i18n value="6cf76f7b-8577-4cfe-b3a1-dc08a9ac24de"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note the following:
-- MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
-- MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
-- MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the **`global_temp`** database**. The **`global_temp`** database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md <i18n value="eda8a0b1-94f8-4c61-bf07-c86a016746ba"/>
-- MAGIC 
-- MAGIC 
-- MAGIC While no job was triggered when defining these views, a job is triggered _each time_ a query is executed against the view.

-- COMMAND ----------

-- MAGIC %md <i18n value="1723a769-272c-47ca-93e8-7b3a9b0674dd"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Clean Up
-- MAGIC Drop the database and all tables to clean up your workspace.

-- COMMAND ----------

DROP DATABASE ${da.schema_name} CASCADE

-- COMMAND ----------

-- MAGIC %md <i18n value="03511454-16d6-40e6-a62a-cd8bdb7de57d"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Synopsis
-- MAGIC 
-- MAGIC In this lab we:
-- MAGIC - Created and deleted databases
-- MAGIC - Explored behavior of managed and external tables
-- MAGIC - Learned about the scoping of views

-- COMMAND ----------

-- MAGIC %md <i18n value="de458d67-efe9-4d5e-87d7-240093072332"/>
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
