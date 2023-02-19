# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="bfb2d018-5d5a-4475-bf1e-293e2a5b0100"/>
# MAGIC 
# MAGIC # Data Engineering with Databricks
# MAGIC 
# MAGIC This course prepares you for topics in the Databricks Certified Associate Data Engineer certification exam.
# MAGIC 
# MAGIC Data professionals from all walks of life will benefit from this comprehensive introduction to the components of the Databricks Lakehouse Platform that directly support putting ETL pipelines into production. You will leverage SQL and Python to define and schedule pipelines that incrementally process new data from a variety of data sources to power analytic applications and dashboards in the Lakehouse. This course offers hands-on instruction in Databricks Data Science & Engineering Workspace, Databricks SQL, Delta Live Tables, Databricks Repos, Databricks Task Orchestration, and the Unity Catalog.
# MAGIC 
# MAGIC **Duration:** 2 full days or 4 half days
# MAGIC 
# MAGIC #### Objectives
# MAGIC - Leverage the Databricks Lakehouse Platform to perform core responsibilities for data pipeline development
# MAGIC - Use SQL and Python to write production data pipelines to extract, transform, and load data into tables and views in the Lakehouse
# MAGIC - Simplify data ingestion and incremental change propagation using Databricks-native features and syntax, including Delta Live Tables
# MAGIC - Orchestrate production pipelines to deliver fresh results for ad-hoc analytics and dashboarding
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC - Basic knowledge of SQL query syntax, including writing queries using `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, and `JOIN`
# MAGIC - Basic knowledge of SQL DDL statements to create, alter, and drop databases and tables
# MAGIC - Basic knowledge of SQL DML statements, including `DELETE`, `INSERT`, `UPDATE`, and `MERGE`
# MAGIC - Experience with or knowledge of data engineering practices on cloud platforms, including cloud features such as virtual machines, object storage, identity management, and metastores

# COMMAND ----------

# MAGIC %md <i18n value="2fbffe4f-04e7-46db-8ed0-af4991565700"/>
# MAGIC 
# MAGIC ## Course Agenda
# MAGIC 
# MAGIC Day 1
# MAGIC 
# MAGIC | Folder | Module Name |
# MAGIC | --- | --- |
# MAGIC | `01 - Databricks Workspace and Services` | Introduction to Databricks Workspace and Services |
# MAGIC | `02 - Delta Lake` | Introduction to Delta Lake |
# MAGIC | `03 - Relational Entities on Databricks` | Relational Entities on Databricks |
# MAGIC | `04 - ETL with Spark SQL` | ETL with Spark SQL |
# MAGIC | `05 - OPTIONAL Python for Spark SQL` | Just Enough Python for Spark SQL |
# MAGIC | `06 - Incremental Data Processing` | Incremental Data Processing with Structured Streaming and Auto Loader |
# MAGIC 
# MAGIC Day 2
# MAGIC 
# MAGIC | Folder | Module Name |
# MAGIC | --- | --- |
# MAGIC | `07 - Multi-Hop Architecture` | Medallion Architecture in the Data Lakehouse |
# MAGIC | `08 - Delta Live Tables` | Using Delta Live Tables |
# MAGIC | `09 - Task Orchestration with Jobs` | Task Orchestration with Databricks Jobs |
# MAGIC | `10 - Running a DBSQL Query` | Running Your First Databricks SQL Query |
# MAGIC | `11 - Managing Permissions` | Managing Permissions in the Lakehouse |
# MAGIC | `12 - Productionalizing Dashboards and Queries in DBSQL` | Productionalizing Dashboards and Queries in Databricks SQL |
# MAGIC 
# MAGIC The notebooks included in each module are listed below.

# COMMAND ----------

# MAGIC %md <i18n value="8bcc5220-9489-49ec-ba62-8260e9871f38"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 01 - Databricks Workspace and Services
# MAGIC * [DE 1.1 - Create and Manage Interactive Clusters]($./01 - Databricks Workspace and Services/DE 1.1 - Create and Manage Interactive Clusters)
# MAGIC * [DE 1.2 - Notebook Basics]($./01 - Databricks Workspace and Services/DE 1.2 - Notebook Basics)
# MAGIC * [DE 1.3L - Getting Started with the Databricks Platform Lab]($./01 - Databricks Workspace and Services/DE 1.3L - Getting Started with the Databricks Platform Lab)

# COMMAND ----------

# MAGIC %md <i18n value="c57df770-302b-4b87-ad3b-c34bdef01029"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 02 - Delta Lake
# MAGIC * [DE 2.1 - Managing Delta Tables]($./02 - Delta Lake/DE 2.1 - Managing Delta Tables)
# MAGIC * [DE 2.2L - Manipulating Tables with Delta Lake Lab]($./02 - Delta Lake/DE 2.2L - Manipulating Tables with Delta Lake Lab)
# MAGIC * [DE 2.3 - Advanced Delta Lake Features]($./02 - Delta Lake/DE 2.3 - Advanced Delta Lake Features)
# MAGIC * [DE 2.4L - Delta Lake Versioning, Optimization, and Vacuuming Lab]($./02 - Delta Lake/DE 2.4L - Delta Lake Versioning, Optimization, and Vacuuming Lab)

# COMMAND ----------

# MAGIC %md <i18n value="6ed0e37b-3299-47fe-b78d-7f7e5fca9396"/>
# MAGIC 
# MAGIC 
# MAGIC ## 03 - Relational Entities on Databricks
# MAGIC * [DE 3.1 - Databases and Tables on Databricks]($./03 - Relational Entities on Databricks/DE 3.1 - Databases and Tables on Databricks)
# MAGIC * [DE 3.2A - Views and CTEs on Databricks]($./03 - Relational Entities on Databricks/DE 3.2A - Views and CTEs on Databricks)
# MAGIC * [DE 3.3L - Databases, Tables & Views Lab]($./03 - Relational Entities on Databricks/DE 3.3L - Databases, Tables & Views Lab)

# COMMAND ----------

# MAGIC %md <i18n value="f958dddc-d0e2-4c21-82ad-db6aaebabda4"/>
# MAGIC 
# MAGIC 
# MAGIC ## 04 - ETL with Spark SQL
# MAGIC * [DE 4.1 - Querying Files Directly]($./04 - ETL with Spark SQL/DE 4.1 - Querying Files Directly)
# MAGIC * [DE 4.2 - Providing Options for External Sources]($./04 - ETL with Spark SQL/DE 4.2 - Providing Options for External Sources)
# MAGIC * [DE 4.3 - Creating Delta Tables]($./04 - ETL with Spark SQL/DE 4.3 - Creating Delta Tables)
# MAGIC * [DE 4.4 - Writing to Tables]($./04 - ETL with Spark SQL/DE 4.4 - Writing to Tables)
# MAGIC * [DE 4.5L - Extract and Load Data Lab]($./04 - ETL with Spark SQL/DE 4.5L - Extract and Load Data Lab)
# MAGIC * [DE 4.6 - Cleaning Data]($./04 - ETL with Spark SQL/DE 4.6 - Cleaning Data)
# MAGIC * [DE 4.7 - Advanced SQL Transformations]($./04 - ETL with Spark SQL/DE 4.7 - Advanced SQL Transformations)
# MAGIC * [DE 4.8 - SQL UDFs and Control Flow]($./04 - ETL with Spark SQL/DE 4.8 - SQL UDFs and Control Flow)
# MAGIC * [DE 4.9L - Reshaping Data Lab]($./04 - ETL with Spark SQL/DE 4.9L - Reshaping Data Lab)

# COMMAND ----------

# MAGIC %md <i18n value="ce1da17d-44fa-47f4-bd66-af6e0c0f179d"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 05 - OPTIONAL Python for Spark SQL
# MAGIC * [DE 5.1 - Python Basics]($./05 - OPTIONAL Python for Spark SQL/DE 5.1 - Python Basics)
# MAGIC * [DE 5.2 - Python Control Flow]($./05 - OPTIONAL Python for Spark SQL/DE 5.2 - Python Control Flow)
# MAGIC * [DE 5.3L - Python for SQL Lab]($./05 - OPTIONAL Python for Spark SQL/DE 5.3L - Python for SQL Lab)

# COMMAND ----------

# MAGIC %md <i18n value="8fbeed27-4056-4024-afc7-859873916b70"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 06 - Incremental Data Processing
# MAGIC * [DE 6.1 - Incremental Data Ingestion with Auto Loader.py]($./06 - Incremental Data Processing/DE 6.1 - Incremental Data Ingestion with Auto Loader)
# MAGIC * [DE 6.2 - Reasoning about Incremental Data.py]($./06 - Incremental Data Processing/DE 6.2 - Reasoning about Incremental Data)
# MAGIC * [DE 6.3L - Using Auto Loader and Structured Streaming with Spark SQL Lab.py]($./06 - Incremental Data Processing/DE 6.3L - Using Auto Loader and Structured Streaming with Spark SQL Lab)

# COMMAND ----------

# MAGIC %md <i18n value="2d9f1a05-fd79-4766-a492-74c21c1d6bad"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 07 - Multi-Hop Architecture
# MAGIC * [DE 7.1 - Incremental Multi-Hop in the Lakehouse]($./07 - Multi-Hop Architecture/DE 7.1 - Incremental Multi-Hop in the Lakehouse)
# MAGIC * [DE 7.2L - Propagating Incremental Updates with Structured Streaming and Delta Lake Lab]($./07 - Multi-Hop Architecture/DE 7.2L - Propagating Incremental Updates with Structured Streaming and Delta Lake Lab)

# COMMAND ----------

# MAGIC %md <i18n value="c633fc58-9310-48e0-bcfa-974b30b75849"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## 08 - Delta Live Tables
# MAGIC * DE 8.1 - DLT
# MAGIC   * [DE 8.1.1 - DLT UI Walkthrough]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.1 - DLT UI Walkthrough)
# MAGIC   * [DE 8.1.2 - SQL for Delta Live Tables]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.2 - SQL for Delta Live Tables)
# MAGIC   * [DE 8.1.3 - Pipeline Results]($./08 - Delta Live Tables/DE 8.1 - DLT/DE 8.1.3 - Pipeline Results)
# MAGIC 
# MAGIC * DE 8.2 - DLT Lab
# MAGIC   * [DE 8.2.1L - Lab Instructions]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.1L - Lab Instructions)
# MAGIC   * [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab)
# MAGIC   * [DE 8.2.3L - Lab Conclusion]($./08 - Delta Live Tables/DE 8.2 - DLT Lab/DE 8.2.3L - Lab Conclusion)

# COMMAND ----------

# MAGIC %md <i18n value="74e6ecb4-6268-4c2c-a6a1-726b02cf392e"/>
# MAGIC 
# MAGIC 
# MAGIC ## 09 - Task Orchestration with Jobs
# MAGIC * DE 9.1 - Scheduling Tasks with the Jobs UI
# MAGIC   * [DE 9.1.1 - Task Orchestration with Databricks Jobs]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.1 - Task Orchestration with Databricks Jobs)
# MAGIC   * [DE 9.1.2 - Reset]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.2 - Reset)
# MAGIC   * [DE 9.1.3 - DLT Job]($./09 - Task Orchestration with Jobs/DE 9.1 - Scheduling Tasks with the Jobs UI/DE 9.1.3 - DLT Job)
# MAGIC 
# MAGIC * DE 9.2L - Jobs Lab
# MAGIC   * [DE 9.2.1L - Lab Instructions]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.1L - Lab Instructions)
# MAGIC   * [DE 9.2.2L - Batch Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.2L - Batch Job)
# MAGIC   * [DE 9.2.3L - DLT Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.3L - DLT Job)
# MAGIC   * [DE 9.2.4L - Query Results Job]($./09 - Task Orchestration with Jobs/DE 9.2L - Jobs Lab/DE 9.2.4L - Query Results Job)

# COMMAND ----------

# MAGIC %md <i18n value="498a81f3-4eda-49ac-aadb-ac33360af496"/>
# MAGIC 
# MAGIC ## 10 - Running a DBSQL Query
# MAGIC * [DE 10.1 - Navigating Databricks SQL and Attaching to Endpoints]($./10 - Running a DBSQL Query/DE 10.1 - Navigating Databricks SQL and Attaching to Endpoints)

# COMMAND ----------

# MAGIC %md <i18n value="ea5067ca-80b5-44d0-9446-8c591649d515"/>
# MAGIC 
# MAGIC 
# MAGIC ## 11 - Managing Permissions
# MAGIC * [DE 11.1 - Managing Permissions for Databases, Tables, and Views]($./11 - Managing Permissions/DE 11.1 - Managing Permissions for Databases, Tables, and Views)
# MAGIC * [DE 11.2L - Configuring Privileges for Production Data and Derived Tables Lab]($./11 - Managing Permissions/DE 11.2L - Configuring Privileges for Production Data and Derived Tables Lab)

# COMMAND ----------

# MAGIC %md <i18n value="76d731fd-01f1-4602-9d7b-4554eed88b8f"/>
# MAGIC 
# MAGIC 
# MAGIC ## 12 - Productionalizing Dashboards and Queries in DBSQL
# MAGIC * [DE 12.1 - Last Mile ETL with DBSQL]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.1 - Last Mile ETL with DBSQL)
# MAGIC * DE 12.2L - OPTIONAL Capstone
# MAGIC   * [DE 12.2.1L - Instructions and Configuration]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.1L - Instructions and Configuration)
# MAGIC   * [DE 12.2.2L - DLT Task]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.2L - DLT Task)
# MAGIC   * [DE 12.2.3L - Land New Data]($./12 - Productionalizing Dashboards and Queries in DBSQL/DE 12.2L - OPTIONAL Capstone/DE 12.2.3L - Land New Data)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
