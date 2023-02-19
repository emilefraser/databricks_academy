-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="73b4cbc8-b2b3-4d51-8443-0280a10127e9"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Views and CTEs on Databricks
-- MAGIC In this demonstration, you will create and explore views and common table expressions (CTEs).
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define views
-- MAGIC * Run queries that use common table expressions
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">Create View - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">Common Table Expressions - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="c297b643-5e56-4ed9-928a-b4261b206461"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Classroom Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.2A

-- COMMAND ----------

-- MAGIC %md <i18n value="f94b665d-e3c5-4dc7-8f40-6e892bdbe71a"/>
-- MAGIC 
-- MAGIC  
-- MAGIC We start by creating a table of data we can use for the demonstration.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md <i18n value="8bc49e5c-12e9-4458-90aa-88b67091f6f7"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC To show a list of tables (and views), we use the **`SHOW TABLES`** command also demonstrated below.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="b80b82c4-c65f-47fe-8968-4c3051f59ba1"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Views, Temp Views & Global Temp Views
-- MAGIC 
-- MAGIC To set this demonstration up, we are going to first create one of each type of view.
-- MAGIC 
-- MAGIC Then in the next notebook, we will explore the differences between how each one behaves.

-- COMMAND ----------

-- MAGIC %md <i18n value="ead94707-a156-4282-9f11-b4976c39470d"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### Views
-- MAGIC Let's create a view that contains only the data where the origin is "ABQ" and the destination is "LAX".

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

-- MAGIC %md <i18n value="f7cc0d7b-eb93-406a-8925-60ea057466ea"/>
-- MAGIC 
-- MAGIC  
-- MAGIC  
-- MAGIC Note that the **`view_delays_abq_lax`** view has been added to the list below:

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="6badc00c-9bf4-47cb-aac8-a474d678e4f6"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ### Temporary Views
-- MAGIC 
-- MAGIC Next we'll create a temporary view. 
-- MAGIC 
-- MAGIC The syntax is very similar but adds **`TEMPORARY`** to the command.

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

-- MAGIC %md <i18n value="b19e8641-b379-4bab-83e7-3aff6dacd8ec"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Now if we show our tables again, we will see the one table and both views.
-- MAGIC 
-- MAGIC Make note of the values in the **`isTemporary`** column.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="7ac13dd9-3f9f-4a41-8945-3405d7a1e86a"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### Global Temp Views
-- MAGIC 
-- MAGIC Lastly, we'll create a global temp view. 
-- MAGIC 
-- MAGIC Here we simply add **`GLOBAL`** to the command. 
-- MAGIC 
-- MAGIC Also note the **`global_temp`** database qualifer in the subsequent **`SELECT`** statement.

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md <i18n value="83ab4417-60d5-4077-8947-ad53d6eb1dce"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Before we move on, review one last time the database's tables and views...

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md <i18n value="acf19ac9-f423-4ce6-85c1-e313672645e2"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ...and the tables and views in the **`global_temp`** database:

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md <i18n value="4b98c78a-c415-4a5c-a4cc-980d28e216b7"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Next we are going to demonstrate how tables and views are persisted across multiple sessions and how temp views are not.
-- MAGIC 
-- MAGIC To do this simply open the next notebook, [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont), and continue with the lesson.
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note: There are several scenarios in which a new session may be created:
-- MAGIC * Restarting a cluster
-- MAGIC * Detaching and reataching to a cluster
-- MAGIC * Installing a python package which in turn restarts the Python interpreter
-- MAGIC * Or simply opening a new notebook

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
