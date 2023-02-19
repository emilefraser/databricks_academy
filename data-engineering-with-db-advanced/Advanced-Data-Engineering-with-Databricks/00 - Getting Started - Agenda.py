# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started
# MAGIC 
# MAGIC In this lesson we will introduce our courseware, various conventions and finally review this course's agenda.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Configure the learning environment by running the **`Classroom-Setup`** scripts.
# MAGIC - Identify Python and Hive variables provided by the courseware.
# MAGIC - Identify utility functions provided by the courseware
# MAGIC - Install the datasets used by this course
# MAGIC - Enumerate the modules and lessons covered by this course.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson.
# MAGIC 
# MAGIC These setup scripts configure different assets in the workspace as needed for each lesson

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Courseware Conventions
# MAGIC 
# MAGIC While not a pattern that is generally recommended, these notebooks will use various Hive variables to substitute in various values.
# MAGIC 
# MAGIC In this example, we can see the database name, your prescribed working directory and the location of your database.
# MAGIC 
# MAGIC These and other values are designed to avoid collisions between each student when creating databases and tables and writing files.
# MAGIC 
# MAGIC The following cell demonstrates this pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.db_name}' as db_name, 
# MAGIC        '${da.paths.working_dir}' as working_dir,
# MAGIC        '${da.paths.user_db}' as user_db_path

# COMMAND ----------

# MAGIC %md Simmilarly, these values are availble in Python as we can see below.

# COMMAND ----------

print(f"User Database:     {DA.db_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"User DB Path:      {DA.paths.user_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC There are two important things to note here:
# MAGIC 1. The difference in case between Python variables and Hive variables: upper vs lower case
# MAGIC 2. The subtle difference in how Python and Hive SQL implement string interpolation: **`{python_variable}`** vs **`${hive_variable}`**

# COMMAND ----------

# MAGIC %md
# MAGIC Throughout this course you will see various references to **`DA...`** and **`da...`**, all a reference to Databricks Academy.
# MAGIC 
# MAGIC In all cases, these values and functions are provided by this course for educational purposes and are not part of any core API.

# COMMAND ----------

# MAGIC %md ## Install Datasets
# MAGIC 
# MAGIC Next, we need to "install" the datasets this course uses by copying them from their current location in the cloud to a location relative to your workspace.
# MAGIC 
# MAGIC All that is required is to run the following cell. 
# MAGIC 
# MAGIC By default, the **`install_datasets()`** function will not reinstall the datasets upon subsequent invocation but this behavior can be adjusted by modifying the parameters below.

# COMMAND ----------

DA.install_datasets(reinstall=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Agenda
# MAGIC 
# MAGIC While the install completes, we can review the Agenda and then start lesson #1.
# MAGIC 
# MAGIC By the time you are ready to start lesson #2, the "install" should have been completed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 00 - Getting Started
# MAGIC * [Getting Started - Agenda]($./00 - Getting Started - Agenda)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 - Architecting for the Lakehouse
# MAGIC * Prerequisite Content - OPTIONAL
# MAGIC   * [ADE 99.1 - Setting Up Tables]($./99 - OPTIONAL Content/ADE 99.1 - Setting Up Tables)
# MAGIC   * [ADE 99.2 - Optimizing Data Storage]($./99 - OPTIONAL Content/ADE 99.2 - Optimizing Data Storage)
# MAGIC   * [ADE 99.3 - Understanding Delta Lake Transactions]($./99 - OPTIONAL Content/ADE 99.3 - Understanding Delta Lake Transactions)
# MAGIC * [ADE 1.1 - Streaming Design Patterns]($./01 - Architecting for the Lakehouse/ADE 1.1 - Streaming Design Patterns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Bronze Ingestion Patterns
# MAGIC * Prerequisite Content - OPTIONAL
# MAGIC   * [ADE 99.4 - Using Clone with Delta Lake]($./99 - OPTIONAL Content/ADE 99.4 - Using Clone with Delta Lake)
# MAGIC   * [ADE 99.5 - Auto Loader]($./99 - OPTIONAL Content/ADE 99.5 - Auto Loader)
# MAGIC * [ADE 2.1 - Auto Load to Multiplex Bronze]($./02 - Bronze Ingestion Patterns/ADE 2.1 - Auto Load to Multiplex Bronze)
# MAGIC * [ADE 2.2 - Streaming from Multiplex Bronze]($./02 - Bronze Ingestion Patterns/ADE 2.2 - Streaming from Multiplex Bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 - Promoting to Silver
# MAGIC * [ADE 3.1 - Streaming Deduplication]($./03 - Promoting to Silver/ADE 3.1 - Streaming Deduplication)
# MAGIC * [ADE 3.2 - Quality Enforcement]($./03 - Promoting to Silver/ADE 3.2 - Quality Enforcement)
# MAGIC * [ADE 3.3 - Promoting to Silver]($./03 - Promoting to Silver/ADE 3.3 - Promoting to Silver)
# MAGIC * [ADE 3.4 - Type 2 SCD]($./03 - Promoting to Silver/ADE 3.4 - Type 2 SCD)
# MAGIC * [ADE 3.5 - Stream Static Join]($./03 - Promoting to Silver/ADE 3.5 - Stream Static Join)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 - Gold Query Layer
# MAGIC * [ADE 4.1 - Stored Views]($./04 - Gold Query Layer/ADE 4.1 - Stored Views)
# MAGIC * [ADE 4.2 - Materialized Gold Tables]($./04 - Gold Query Layer/ADE 4.2 - Materialized Gold Tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 - Storing Data Securely
# MAGIC * [ADE 5.1 - PII Lookup Table]($./05 - Storing Data Securely/ADE 5.1 - PII Lookup Table)
# MAGIC * [ADE 5.2 - Storing PII Securely]($./05 - Storing Data Securely/ADE 5.2 - Storing PII Securely)
# MAGIC * [ADE 5.3 - Deidentified PII Access]($./05 - Storing Data Securely/ADE 5.3 - Deidentified PII Access)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 - Propagating Updates and Deletes
# MAGIC * [ADE 6.1 - Processing Records from Change Data Feed]($./06 - Propagating Updates and Deletes/ADE 6.1 - Processing Records from Change Data Feed)
# MAGIC * [ADE 6.2 - Propagating Deletes with CDF]($./06 - Propagating Updates and Deletes/ADE 6.2 - Propagating Deletes with CDF)
# MAGIC * [ADE 6.3 - Deleting at Partition Boundaries]($./06 - Propagating Updates and Deletes/ADE 6.3 - Deleting at Partition Boundaries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 - Orchestration and Scheduling
# MAGIC 
# MAGIC * ADE 7.1 - Multi-Task Jobs
# MAGIC   * [Task-1, Create Database]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-1, Create Database)
# MAGIC   * [Task-2, From Task 2]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-2, From Task 2)
# MAGIC   * [Task-3, From Task 3]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-3, From Task 3)
# MAGIC   * [Task-4, Key-Param]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-4, Key-Param)
# MAGIC   * [Task-5, Create task_5]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-5, Create task_5)
# MAGIC   * [Task-6, Errors]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-6, Errors)
# MAGIC   * [Task-7, Cleanup]($./07 - Orchestration and Scheduling/ADE 7.1 - Multi-Task Jobs/Task-7, Cleanup)
# MAGIC * [ADE 7.2 - CLI and REST API]($./07 - Orchestration and Scheduling/ADE 7.2 - CLI and REST API)
# MAGIC * ADE 7.3 - Deploying Workloads
# MAGIC   * [1 - Reset Pipelines]($./07 - Orchestration and Scheduling/ADE 7.3 - Deploying Workloads/1 - Reset Pipelines)
# MAGIC   * [2 - Schedule Streaming Jobs]($./07 - Orchestration and Scheduling/ADE 7.3 - Deploying Workloads/2 - Schedule Streaming Jobs)
# MAGIC   * [3 - Schedule Batch Jobs]($./07 - Orchestration and Scheduling/ADE 7.3 - Deploying Workloads/3 - Schedule Batch Jobs)
# MAGIC   * [4 - Streaming Progress]($./07 - Orchestration and Scheduling/ADE 7.3 - Deploying Workloads/4 - Streaming Progress)
# MAGIC   * [5 - Demo Conclusion]($./07 - Orchestration and Scheduling/ADE 7.3 - Deploying Workloads/5 - Demo Conclusion)
# MAGIC * Additional Content - OPTIONAL
# MAGIC   * [ADE 99.6 - Error Prone]($./99 - OPTIONAL Content/ADE 99.6 - Error Prone)
# MAGIC   * [ADE 99.7 - Refactor to Relative Imports]($./99 - OPTIONAL Content/ADE 99.7 - Refactor to Relative Imports)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
