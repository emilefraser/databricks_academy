# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="1fb32f72-2ccc-4206-98d9-907287fc3262"/>
# MAGIC 
# MAGIC 
# MAGIC # Using the Delta Live Tables UI
# MAGIC 
# MAGIC This demo will explore the DLT UI. 
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Deploy a DLT pipeline
# MAGIC * Explore the resultant DAG
# MAGIC * Execute an update of the pipeline
# MAGIC * Look at metrics

# COMMAND ----------

# MAGIC %md <i18n value="c950ed75-9a93-4340-a82c-e00505222d15"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC 
# MAGIC The following cell is configured to reset this demo.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.1.1

# COMMAND ----------

# MAGIC %md <i18n value="0a719ade-b4b5-49b5-89bf-8fc2b0b7d63c"/>
# MAGIC 
# MAGIC 
# MAGIC Execute the following cell to print out values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="71b010a3-80be-4909-9b44-6f68029f16c0"/>
# MAGIC 
# MAGIC ## Create Pipeline
# MAGIC 
# MAGIC In this section, you will create a pipeline using a notebook provided with the courseware. We'll explore the contents of the notebook in the following lesson.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Delta Live Tables** tab, and click **Create Pipeline**. 
# MAGIC 2. Configure the pipeline settings specified below.
# MAGIC 
# MAGIC   **NOTE:** You'll need the values provided in the cell output above to configure some of these settings.
# MAGIC 
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Pipeline name | Enter the **Pipeline Name** provided above |
# MAGIC | Product edition | Choose **Advanced** |
# MAGIC | Pipeline mode | Choose **Triggered** |
# MAGIC | Cluster policy | Choose the **Policy** provided above |
# MAGIC | Notebook libraries | Use the navigator to select or enter the **Notebook Path** provided above |
# MAGIC | Storage location | Enter the **Storage Location** provided above |
# MAGIC | Target schema | Enter the **Target** database name provided above |
# MAGIC | Cluster mode | Choose **Fixed size** to disable auto scaling for your cluster |
# MAGIC | Workers | Enter **0** to use a Single Node cluster |
# MAGIC | Photon Acceleration | Uncheck this checkbox to disable |
# MAGIC | Configuration | Click **Advanced** to view additional settings<br>Click **Add Configuration** to input the **Key** and **Value** for row #1 in the table below<br>Click **Add Configuration** to input the **Key** and **Value** for row #2 in the table below |
# MAGIC | Channel | Choose **Current** to use the current runtime version |
# MAGIC 
# MAGIC | Configuration | Key                 | Value                                      |
# MAGIC | ------------- | ------------------- | ------------------------------------------ |
# MAGIC | #1            | **`spark.master`**  | **`local[*]`**                             |
# MAGIC | #2            | **`datasets_path`** | Enter the **Datasets Path** provided above |
# MAGIC 
# MAGIC Finally, click **Create**.
# MAGIC 
# MAGIC 
# MAGIC A few notes on pipeline settings:
# MAGIC 
# MAGIC - **Pipeline mode** - This specifies how the pipeline will be run. Choose the mode based on latency and cost requirements.
# MAGIC   - `Triggered` pipelines run once and then shut down until the next manual or scheduled update.
# MAGIC   - `Continuous` pipelines run continuously, ingesting new data as it arrives.
# MAGIC - **Notebook libraries** - Even though this document is a standard Databricks Notebook, the SQL syntax is specialized to DLT table declarations. We will be exploring the syntax in the exercise that follows.
# MAGIC - **Storage location** - This optional field allows the user to specify a location to store logs, tables, and other information related to pipeline execution. If not specified, DLT will automatically generate a directory.
# MAGIC - **Target** - If this optional field is not specified, tables will not be registered to a metastore, but will still be available in the DBFS. See <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentation</a> for more information on this option.
# MAGIC - **Cluster mode**, **Min Workers**, **Max Workers** - These fields control the worker configuration for the underlying cluster processing the pipeline. Here, we set the number of workers to 0. This works in conjunction with the **spark.master** parameter defined above to configure the cluster as a Single Node cluster.

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="a7e4b2fc-83a1-4509-8269-9a4c5791de21"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run a Pipeline
# MAGIC 
# MAGIC With a pipeline created, you will now run the pipeline.
# MAGIC 
# MAGIC 1. Select **Development** to run the pipeline in development mode. 
# MAGIC   * Development mode provides for more expeditious iterative development by reusing the cluster (as opposed to creating a new cluster for each run) and disabling retries so that you can readily identify and fix errors.
# MAGIC   * Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentation</a> for more information on this feature.
# MAGIC 2. Click **Start**.
# MAGIC 
# MAGIC The initial run will take several minutes while a cluster is provisioned. 
# MAGIC 
# MAGIC Subsequent runs will be appreciably quicker.

# COMMAND ----------

# MAGIC %md <i18n value="4b92f93e-7a7f-4169-a1d2-9df3ac440674"/>
# MAGIC 
# MAGIC 
# MAGIC ## Exploring the DAG
# MAGIC 
# MAGIC As the pipeline completes, the execution flow is graphed. 
# MAGIC 
# MAGIC Selecting the tables reviews the details.
# MAGIC 
# MAGIC Select **sales_orders_cleaned**. Notice the results reported in the **Data Quality** section. Because this flow has data expectations declared, those metrics are tracked here. No records are dropped because the constraint is declared in a way that allows violating records to be included in the output. This will be covered in more details in the next exercise.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
