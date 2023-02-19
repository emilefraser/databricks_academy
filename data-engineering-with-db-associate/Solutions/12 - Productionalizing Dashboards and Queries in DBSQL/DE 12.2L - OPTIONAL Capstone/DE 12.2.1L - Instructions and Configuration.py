# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="01f3c782-1973-4a69-812a-7f9721099941"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## End-to-End ETL in the Lakehouse
# MAGIC 
# MAGIC In this notebook, you will pull together concepts learned throughout the course to complete an example data pipeline.
# MAGIC 
# MAGIC The following is a non-exhaustive list of skills and tasks necessary to successfully complete this exercise:
# MAGIC * Using Databricks notebooks to write queries in SQL and Python
# MAGIC * Creating and modifying databases, tables, and views
# MAGIC * Using Auto Loader and Spark Structured Streaming for incremental data processing in a multi-hop architecture
# MAGIC * Using Delta Live Table SQL syntax
# MAGIC * Configuring a Delta Live Table pipeline for continuous processing
# MAGIC * Using Databricks Jobs to orchestrate tasks from notebooks stored in Repos
# MAGIC * Setting chronological scheduling for Databricks Jobs
# MAGIC * Defining queries in Databricks SQL
# MAGIC * Creating visualizations in Databricks SQL
# MAGIC * Defining Databricks SQL dashboards to review metrics and results

# COMMAND ----------

# MAGIC %md <i18n value="f9cf3bbc-aa6a-45c2-9d26-a3785e350e1f"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset all the databases and directories associated with this lab.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.1L

# COMMAND ----------

# MAGIC %md <i18n value="3fe92b6e-3e10-4771-8eef-8f4b060dd48f"/>
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="806818f8-e931-45ba-b86f-d65cdf76f215"/>
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a DLT Pipeline
# MAGIC **NOTE**: The main difference between the instructions here and in previous labs with DLT is that in this instance, we will be setting up our pipeline for **Continuous** execution in **Production** mode.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="e1663032-caa8-4b99-af1a-3ab27deaf130"/>
# MAGIC 
# MAGIC 
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
# MAGIC 
# MAGIC This should begin the deployment of infrastructure.

# COMMAND ----------

# ANSWER
 
# This function is provided for students who do not 
# want to work through the exercise of creating the pipeline.
DA.create_pipeline()

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="6c8bd13c-938a-4283-b15a-bc1a598fb070"/>
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC Our DLT pipeline is setup to process data as soon as it arrives. 
# MAGIC 
# MAGIC We'll schedule a notebook to land a new batch of data each minute so we can see this functionality in action.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md <i18n value="df989e07-97d4-4a34-9729-fad02399a908"/>
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Land-Data** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Path**, select the **Notebook Path** value provided in the cell above
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`Land-Data`** (the defaulted value) to the **Job Name** provided for you in the previous cell.    
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md <i18n value="3994f3ee-e335-48c7-8770-64e1ef0dfab7"/>
# MAGIC 
# MAGIC 
# MAGIC ## Set a Chronological Schedule for your Job
# MAGIC Steps:
# MAGIC 1. Locate the **Schedule** section in the side panel on the right.
# MAGIC 1. Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 1. Change the **Schedule type** field from **Manual (Paused)** to **Scheduled**, which will bring up a chron scheduling UI.
# MAGIC 1. Set the schedule to update **Every 2**, **Minutes** from **0 minutes past the hour** 
# MAGIC 1. Click **Save**
# MAGIC 
# MAGIC **NOTE**: If you wish, you can click **Run now** to trigger the first run, or wait until the top of the next minute to make sure your scheduling has worked successfully.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job()

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# ANSWER

# This function is provided to start the job and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md <i18n value="30df4ffa-22b9-4e2c-b8d8-54aa09a8d4ed"/>
# MAGIC 
# MAGIC 
# MAGIC ## Register DLT Event Metrics for Querying with DBSQL
# MAGIC 
# MAGIC The following cell prints out SQL statements to register the DLT event logs to your target database for querying in DBSQL.
# MAGIC 
# MAGIC Execute the output code with the DBSQL Query Editor to register these tables and views. 
# MAGIC 
# MAGIC Explore each and make note of the logged event metrics.

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# MAGIC %md <i18n value="e035ddc7-4af9-4e9c-81f8-530e8db7c504"/>
# MAGIC 
# MAGIC 
# MAGIC ## Define a Query on the Gold Table
# MAGIC 
# MAGIC The **daily_patient_avg** table is automatically updated each time a new batch of data is processed through the DLT pipeline. Each time a query is executed against this table, DBSQL will confirm if there is a newer version and then materialize results from the newest available version.
# MAGIC 
# MAGIC Run the following cell to print out a query with your database name. Save this as a DBSQL query.

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md <i18n value="679db36c-b257-4248-b2fe-56b85099d0b9"/>
# MAGIC 
# MAGIC 
# MAGIC ## Add a Line Plot Visualization
# MAGIC 
# MAGIC To track trends in patient averages over time, create a line plot and add it to a new dashboard.
# MAGIC 
# MAGIC Create a line plot with the following settings:
# MAGIC * **X Column**: **`date`**
# MAGIC * **Y Column**: **`avg_heartrate`**
# MAGIC * **Group By**: **`name`**
# MAGIC 
# MAGIC Add this visualization to a dashboard.

# COMMAND ----------

# MAGIC %md <i18n value="7351e179-68f8-4091-a6ee-647974f010ce"/>
# MAGIC 
# MAGIC 
# MAGIC ## Track Data Processing Progress
# MAGIC 
# MAGIC The code below extracts the **`flow_name`**, **`timestamp`**, and **`num_output_rows`** from the DLT event logs.
# MAGIC 
# MAGIC Save this query in DBSQL, then define a bar plot visualization that shows:
# MAGIC * **X Column**: **`timestamp`**
# MAGIC * **Y Column**: **`num_output_rows`**
# MAGIC * **Group By**: **`flow_name`**
# MAGIC 
# MAGIC Add your visualization to your dashboard.

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md <i18n value="5f94b102-d42e-40f1-8253-c14cbf86d717"/>
# MAGIC 
# MAGIC 
# MAGIC ## Refresh your Dashboard and Track Results
# MAGIC 
# MAGIC The **Land-Data** notebook scheduled with Jobs above has 12 batches of data, each representing a month of recordings for our small sampling of patients. As configured per our instructions, it should take just over 20 minutes for all of these batches of data to be triggered and processed (we scheduled the Databricks Job to run every 2 minutes, and batches of data will process through our pipeline very quickly after initial ingestion).
# MAGIC 
# MAGIC Refresh your dashboard and review your visualizations to see how many batches of data have been processed. (If you followed the instructions as outlined here, there should be 12 distinct flow updates tracked by your DLT metrics.) If all source data has not yet been processed, you can go back to the Databricks Jobs UI and manually trigger additional batches.

# COMMAND ----------

# MAGIC %md <i18n value="b61bf387-2c1b-4ae6-8968-c4189beb477f"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC With everything configured, you can now continue to the final part of your lab in the notebook [DE 12.2.4L - Final Steps]($./DE 12.2.4L - Final Steps)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
