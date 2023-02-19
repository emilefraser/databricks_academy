# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="36722caa-e827-436b-8c45-3e85619fd2d0"/>
# MAGIC 
# MAGIC 
# MAGIC # Orchestrating Jobs with Databricks Workflows
# MAGIC 
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC 
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook task as a triggered standalone job, and then add a dependent task using a DLT pipeline. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Schedule a notebook task in a Databricks Workflow Job
# MAGIC * Describe job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Schedule a DLT pipeline task in a Databricks Workflow Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-09.1.1

# COMMAND ----------

# MAGIC %md <i18n value="f1dc94ee-1f34-40b1-b2ba-49de9801b0d1"/>
# MAGIC 
# MAGIC 
# MAGIC ## Create and configure a pipeline
# MAGIC The pipeline we create here is nearly identical to the one in the previous unit.
# MAGIC 
# MAGIC We will use it as part of a scheduled job in this lesson.
# MAGIC 
# MAGIC Execute the following cell to print out the values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="b1f23965-ab36-40da-8907-e8f1fdc53aed"/>
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Delta Live Tables** tab, and click **Create Pipeline**. 
# MAGIC 2. Configure the pipeline as specified below. You'll need the values provided in the cell output above for this step.
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
# MAGIC <br>
# MAGIC 
# MAGIC 3. Click the **Create** button.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: We won't be executing this pipeline directly as it will be executed by our job later in this lesson, but if you want to test it real quick, you can click the **Start** button now.

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="ed9ed553-77e7-4ff2-a9dc-12466e30c994"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

DA.print_job_config_task_reset()

# COMMAND ----------

# MAGIC %md <i18n value="8c3c501e-0334-412a-91b3-bf250dfe8856"/>
# MAGIC 
# MAGIC Here, we'll start by scheduling the next notebook.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Jobs** tab, and click the **Create Job** button.
# MAGIC 2. Configure the job and task as specified below. You'll need the values provided in the cell output above for this step.
# MAGIC 
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Reset** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to select or enter the **Reset Notebook Path** provided above |
# MAGIC | Cluster | From the dropdown menu, under **Existing All Purpose Clusters**, select your cluster |
# MAGIC | Job name | In the top-left of the screen, enter the **Job Name** provided above to add a name for the job (not the task) |
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 3. Click the **Create** button.
# MAGIC 4. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md <i18n value="8ebdf7c7-4b4a-49a9-b9d4-25dff82ed169"/>
# MAGIC 
# MAGIC 
# MAGIC ## Cron Scheduling of Databricks Jobs
# MAGIC 
# MAGIC Note that on the right hand side of the Jobs UI, directly under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 
# MAGIC Select the **Add schedule** button to explore scheduling options.
# MAGIC 
# MAGIC Changing the **Trigger type** from **None (Manual)** to **Scheduled** will bring up a cron scheduling UI.
# MAGIC 
# MAGIC This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 
# MAGIC At this time, we'll leave our job set to **Manual** scheduling; select **Cancel** to return to Job details.

# COMMAND ----------

# MAGIC %md <i18n value="50665a01-dd6c-4767-b8ef-56ee02dbd9db"/>
# MAGIC 
# MAGIC 
# MAGIC ## Review Run
# MAGIC 
# MAGIC As currently configured, our single notebook provides identical performance to the legacy Databricks Jobs UI, which only allowed a single notebook to be scheduled.
# MAGIC 
# MAGIC To Review the Job Run
# MAGIC 1. Select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC 1. Find your job. If **the job is still running**, it will be under the **Active runs** section. If **the job finished running**, it will be under the **Completed runs** section
# MAGIC 1. Open the Output details by clicking on the timestamp field under the **Start time** column
# MAGIC 1. If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel
# MAGIC   
# MAGIC The notebook employs the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC 
# MAGIC The actual outcome of the scheduled notebook is to reset the environment for our new job and pipeline.

# COMMAND ----------

# MAGIC %md <i18n value="3dbff1a3-1c13-46f9-91c4-55aefb95be20"/>
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
# MAGIC     1. The **Depends on** field defaults to your previously defined task, **Reset** - leave this value as-is.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`Reset`** task will be at the top, leading into your **`DLT`** task. 
# MAGIC 
# MAGIC This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Click **Run now** to execute your job.
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your job and pipeline is deployed.

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# MAGIC %md <i18n value="4fecba69-f1cf-4413-8bc6-7b50d32b2456"/>
# MAGIC 
# MAGIC 
# MAGIC ## Review Multi-Task Run Results
# MAGIC 
# MAGIC Select the **Runs** tab again and then the most recent run under **Active runs** or **Completed runs** depending on if the job has completed or not.
# MAGIC 
# MAGIC The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failures occur. 
# MAGIC 
# MAGIC Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC 
# MAGIC You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps; note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC 
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
