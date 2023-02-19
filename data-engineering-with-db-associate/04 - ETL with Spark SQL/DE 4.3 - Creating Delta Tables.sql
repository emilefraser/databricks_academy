-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="25ac8d75-ee97-4c88-8028-94ba991d0dba"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Creating Delta Tables
-- MAGIC 
-- MAGIC After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged.
-- MAGIC 
-- MAGIC While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.
-- MAGIC 
-- MAGIC This lesson will focus primarily on the pattern used to create most tables, **`CREATE TABLE _ AS SELECT`** (CTAS) statements.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use CTAS statements to create Delta Lake tables
-- MAGIC - Create new tables from existing views or tables
-- MAGIC - Enrich loaded data with additional metadata
-- MAGIC - Declare table schema with generated columns and descriptive comments
-- MAGIC - Set advanced options to control data location, quality enforcement, and partitioning

-- COMMAND ----------

-- MAGIC %md <i18n value="ae119ec7-0185-469d-8986-75c0e3d0a68f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04.3

-- COMMAND ----------

-- MAGIC %md <i18n value="26c0731b-b738-4034-acdc-0dd2761031e4"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC 
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md <i18n value="db0df24b-95f8-45bc-8651-44e4af4537e2"/>
-- MAGIC 
-- MAGIC  
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC 
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.
-- MAGIC 
-- MAGIC CTAS statements also do not support specifying additional file options.
-- MAGIC 
-- MAGIC We can see how this would present significant limitations when trying to ingest data from CSV files.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md <i18n value="2d99fed5-4e51-45ea-bcbf-e6da504c5e93"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To correctly ingest this data to a Delta Lake table, we'll need to use a reference to the files that allows us to specify options.
-- MAGIC 
-- MAGIC In the previous lesson, we showed doing this by registering an external table. Here, we'll slightly evolve this syntax to specify the options to a temporary view, and then use this temp view as the source for a CTAS statement to successfully register the Delta table.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md <i18n value="3f6875ab-5363-43df-8c36-faeff2933cbe"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Filtering and Renaming Columns from Existing Tables
-- MAGIC 
-- MAGIC Simple transformations like changing column names or omitting columns from target tables can be easily accomplished during table creation.
-- MAGIC 
-- MAGIC The following statement creates a new table containing a subset of columns from the **`sales`** table. 
-- MAGIC 
-- MAGIC Here, we'll presume that we're intentionally leaving out information that potentially identifies the user or that provides itemized purchase details. We'll also rename our fields with the assumption that a downstream system has different naming conventions than our source data.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md <i18n value="e2eb9bb3-e6ec-4842-85a9-895007411f02"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note that we could have accomplished this same goal with a view, as shown below.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md <i18n value="5961066a-e322-415f-9ddf-0cc34247f5cf"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Declare Schema with Generated Columns
-- MAGIC 
-- MAGIC As noted previously, CTAS statements do not support schema declaration. We note above that the timestamp column appears to be some variant of a Unix timestamp, which may not be the most useful for our analysts to derive insights. This is a situation where generated columns would be beneficial.
-- MAGIC 
-- MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table (introduced in DBR 8.3).
-- MAGIC 
-- MAGIC The code below demonstrates creating a new table while:
-- MAGIC 1. Specifying column names and types
-- MAGIC 1. Adding a <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">generated column</a> to calculate the date
-- MAGIC 1. Providing a descriptive column comment for the generated column

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp LONG, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md <i18n value="13084a7f-10a0-453f-aa53-1f75a7e74dd9"/>
-- MAGIC 
-- MAGIC  
-- MAGIC 
-- MAGIC Because **`date`** is a generated column, if we write to **`purchase_dates`** without providing values for the **`date`** column, Delta Lake automatically computes them.
-- MAGIC 
-- MAGIC **NOTE**: The cell below configures a setting to allow for generating columns when using a Delta Lake **`MERGE`** statement. We'll see more on this syntax later in the course.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md <i18n value="4d535b90-f71e-49ac-8101-45f328ec0349"/>
-- MAGIC 
-- MAGIC  
-- MAGIC We can see below that all dates were computed correctly as data was inserted, although neither our source data or insert query specified the values in this field.
-- MAGIC 
-- MAGIC As with any Delta Lake source, the query automatically reads the most recent snapshot of the table for any query; you never need to run **`REFRESH TABLE`**.

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- MAGIC %md <i18n value="7035036b-6829-4a4e-bbea-9c9bbdc85776"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC It's important to note that if a field that would otherwise be generated is included in an insert to a table, this insert will fail if the value provided does not exactly match the value that would be derived by the logic used to define the generated column.
-- MAGIC 
-- MAGIC We can see this error by uncommenting and running the cell below:

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md <i18n value="f8f1d9d6-be28-493e-9736-c6384cbbe944"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Add a Table Constraint
-- MAGIC 
-- MAGIC The error message above refers to a **`CHECK constraint`**. Generated columns are a special implementation of check constraints.
-- MAGIC 
-- MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
-- MAGIC 
-- MAGIC Databricks currently support two types of constraints:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
-- MAGIC 
-- MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
-- MAGIC 
-- MAGIC Below, we'll add a **`CHECK`** constraint to the **`date`** column of our table. Note that **`CHECK`** constraints look like standard **`WHERE`** clauses you might use to filter a dataset.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md <i18n value="604861c2-0d45-434e-be65-41c1da7b2bbf"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Table constraints are shown in the **`TBLPROPERTIES`** field.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md <i18n value="3de6a201-9b87-4d79-9bb1-4fc2f122d55e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Enrich Tables with Additional Options and Metadata
-- MAGIC 
-- MAGIC So far we've only scratched the surface as far as the options for enriching Delta Lake tables.
-- MAGIC 
-- MAGIC Below, we show evolving a CTAS statement to include a number of additional configurations and metadata.
-- MAGIC 
-- MAGIC Our **`SELECT`** clause leverages two built-in Spark SQL commands useful for file ingestion:
-- MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
-- MAGIC * **`input_file_name()`** records the source data file for each record in the table
-- MAGIC 
-- MAGIC We also include logic to create a new date column derived from timestamp data in the source.
-- MAGIC 
-- MAGIC The **`CREATE TABLE`** clause contains several options:
-- MAGIC * A **`COMMENT`** is added to allow for easier discovery of table contents
-- MAGIC * A **`LOCATION`** is specified, which will result in an external (rather than managed) table
-- MAGIC * The table is **`PARTITIONED BY`** a date column; this means that the data from each date will exist within its own directory in the target storage location
-- MAGIC 
-- MAGIC **NOTE**: Partitioning is shown here primarily to demonstrate syntax and impact. Most Delta Lake tables (especially small-to-medium sized data) will not benefit from partitioning. Because partitioning physically separates data files, this approach can result in a small files problem and prevent file compaction and efficient data skipping. The benefits observed in Hive or HDFS do not translate to Delta Lake, and you should consult with an experienced Delta Lake architect before partitioning tables.
-- MAGIC 
-- MAGIC **As a best practice, you should default to non-partitioned tables for most use cases when working with Delta Lake.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md <i18n value="c1aa43f4-9681-4104-824d-c4ce4bc72914"/>
-- MAGIC 
-- MAGIC  
-- MAGIC The metadata fields added to the table provide useful information to understand when records were inserted and from where. This can be especially helpful if troubleshooting problems in the source data becomes necessary.
-- MAGIC 
-- MAGIC All of the comments and properties for a given table can be reviewed using **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC 
-- MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md <i18n value="afad6329-9739-4397-b410-7b49ad9118ce"/>
-- MAGIC 
-- MAGIC  
-- MAGIC Listing the location used for the table reveals that the unique values in the partition column **`first_touch_date`** are used to create data directories.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md <i18n value="ac7e881c-3076-4997-8dcc-4daa5c84a226"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Cloning Delta Lake Tables
-- MAGIC Delta Lake has two options for efficiently copying Delta Lake tables.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md <i18n value="a42c51a7-ac07-4e67-94ea-ed06b6db126c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Because all the data files must be copied over, this can take quite a while for large datasets.
-- MAGIC 
-- MAGIC If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, **`SHALLOW CLONE`** can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md <i18n value="e9b91c6a-2581-4a0b-8f84-2e100f053980"/>
-- MAGIC 
-- MAGIC 
-- MAGIC In either case, data modifications applied to the cloned version of the table will be tracked and stored separately from the source. Cloning is a great way to set up tables for testing SQL code while still in development.

-- COMMAND ----------

-- MAGIC %md <i18n value="64bb6112-b07c-424c-a668-da4cfa29be1f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC 
-- MAGIC In this notebook, we focused primarily on DDL and syntax for creating Delta Lake tables. In the next notebook, we'll explore options for writing updates to tables.

-- COMMAND ----------

-- MAGIC %md <i18n value="c78f9f19-f13b-49b4-9ad0-20181528f924"/>
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
