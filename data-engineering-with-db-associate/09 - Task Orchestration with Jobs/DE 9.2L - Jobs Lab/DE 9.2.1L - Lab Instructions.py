# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="51f698bd-880b-4a85-b187-9b96d8c2cf18"/>
# MAGIC 
# MAGIC 
# MAGIC # Lab: Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC In this lab, you'll be configuring a multi-task job comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries the gold table produced by this pipeline as well as various metrics output by DLT
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Schedule a notebook as a task in a Databricks Job
# MAGIC * Schedule a DLT pipeline as a task in a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-09.2.1L

# COMMAND ----------

# MAGIC %md <i18n value="b7163714-376c-41fd-8e38-80a7247fa923"/>
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="6bc33560-37f4-4f91-910d-669a1708ba66"/>
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC The pipeline we create here is nearly identical to the one in the previous unit.
# MAGIC 
# MAGIC We will use it as part of a scheduled job in this lesson.
# MAGIC 
# MAGIC Execute the following cell to print out the values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="c8b235db-10cf-4a56-92d9-330b80da4f0f"/>
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Delta Live Tables** tab, and click **Create Pipeline**. 
# MAGIC 2. Configure the pipeline settings specified below.
# MAGIC 
# MAGIC **NOTE:** You'll need the values provided in the cell output above to configure some of these settings.
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
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: we won't be executing this pipline directly as it will be executed by our job later in this lesson,<br/>
# MAGIC but if you want to test it real quick, you can click the **Start** button now.

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="f98768ac-cbcc-42a2-8c51-ffdc3778aa11"/>
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md <i18n value="fab2a427-5d5a-4a82-8947-c809d815c2a3"/>
# MAGIC 
# MAGIC Here, we'll start by scheduling the first notebook.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar
# MAGIC 1. Select the **Jobs** tab.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Batch-Job** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Source**, select **Workspace**
# MAGIC     1. For **Path**, specify the **Batch Notebook Path** value provided in the cell above
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen, specify a name for the job (not the task) using the **`Batch-Job`** value provided in the cell above.
# MAGIC 1. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md <i18n value="1ab345ce-dff4-4a99-ad45-209793ddc581"/>
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **DLT** for the task name
# MAGIC     1. For **Type**, select  **Delta Live Tables pipeline**
# MAGIC     1. For **Pipeline**, select the DLT pipeline you configured previously in this exercise<br/>
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **Batch-Job** - leave this value as-is.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`Batch-Job`** task will be at the top, leading into your **`DLT`** task.

# COMMAND ----------

# MAGIC %md <i18n value="dd4e16c5-1842-4642-8159-117cfc84d4b4"/>
# MAGIC 
# MAGIC 
# MAGIC ## Schedule an Additional Notebook Task
# MAGIC 
# MAGIC An additional notebook has been provided which queries some of the DLT metrics and the gold table defined in the DLT pipeline. 
# MAGIC 
# MAGIC We'll add this as a final task in our job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC Steps:
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Query-Results** for the task name
# MAGIC     1. For **Type**, select **Notebook**
# MAGIC     1. For **Path**, select the **Query Notebook Path** value provided in the cell above
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **DLT** - leave this value as-is.
# MAGIC     1. Click the blue **Create task** button
# MAGIC     
# MAGIC Click the blue **Run now** button in the top right of the screen to run this job.
# MAGIC 
# MAGIC From the **Runs** tab, you will be able to click on the start time for this run under the **Active runs** section and visually track task progress.
# MAGIC 
# MAGIC Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
