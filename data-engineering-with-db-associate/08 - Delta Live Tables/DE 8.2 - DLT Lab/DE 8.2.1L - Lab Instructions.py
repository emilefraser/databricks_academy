# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="2eb97b71-b2ab-4b68-afdc-1663ec49e9d4"/>
# MAGIC 
# MAGIC 
# MAGIC # Lab: Migrating SQL Notebooks to Delta Live Tables
# MAGIC 
# MAGIC This notebook describes the overall structure for the lab exercise, configures the environment for the lab, provides simulated data streaming, and performs cleanup once you are done. A notebook like this is not typically needed in a production pipeline scenario.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Convert existing data pipelines to Delta Live Tables

# COMMAND ----------

# MAGIC %md <i18n value="782da0e9-5fc2-4deb-b7a4-939af49e38ed"/>
# MAGIC 
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md <i18n value="b691e21b-24a5-46bc-97d8-a43e9ae6e268"/>
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Begin by running the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.2.1L

# COMMAND ----------

# MAGIC %md <i18n value="c68290ac-56ad-4d6e-afec-b0a61c35386f"/>
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with more data before proceeding.
# MAGIC 
# MAGIC You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="7cb98302-06c2-4384-bdf7-2260cbf2662d"/>
# MAGIC 
# MAGIC 
# MAGIC Execute the following cell to print out values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()    

# COMMAND ----------

# MAGIC %md <i18n value="784d3bc4-5c4e-4ef8-ab56-3ebaa92238b0"/>
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
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

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="3340e93d-1fad-4549-bf79-ec239b1d59d4"/>
# MAGIC 
# MAGIC 
# MAGIC ## Open and Complete DLT Pipeline Notebook
# MAGIC 
# MAGIC You will perform your work in the companion notebook [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab),<br/>
# MAGIC which you will ultimately deploy as a pipeline.
# MAGIC 
# MAGIC Open the Notebook and, following the guidelines provided therein, fill in the cells where prompted to<br/>
# MAGIC implement a multi-hop architecture similar to the one we worked with in the previous section.

# COMMAND ----------

# MAGIC %md <i18n value="90a66079-16f8-4503-ab48-840cbdd07914"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run your Pipeline
# MAGIC 
# MAGIC Select **Development** mode, which accelerates the development lifecycle by reusing the same cluster across runs.<br/>
# MAGIC It will also turn off automatic retries when jobs fail.
# MAGIC 
# MAGIC Click **Start** to begin the first update to your table.
# MAGIC 
# MAGIC Delta Live Tables will automatically deploy all the necessary infrastructure and resolve the dependencies between all datasets.
# MAGIC 
# MAGIC **NOTE**: The first table update may take several minutes as relationships are resolved and infrastructure deploys.

# COMMAND ----------

# MAGIC %md <i18n value="d1797d22-692c-43ce-b146-1e0248e65da3"/>
# MAGIC 
# MAGIC 
# MAGIC ## Troubleshooting Code in Development Mode
# MAGIC 
# MAGIC Don't despair if your pipeline fails the first time. Delta Live Tables is in active development, and error messages are improving all the time.
# MAGIC 
# MAGIC Because relationships between tables are mapped as a DAG, error messages will often indicate that a dataset isn't found.
# MAGIC 
# MAGIC Let's consider our DAG below:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/dlt-dag.png">
# MAGIC 
# MAGIC If the error message **`Dataset not found: 'recordings_parsed'`** is raised, there may be several culprits:
# MAGIC 1. The logic defining **`recordings_parsed`** is invalid
# MAGIC 1. There is an error reading from **`recordings_bronze`**
# MAGIC 1. A typo exists in either **`recordings_parsed`** or **`recordings_bronze`**
# MAGIC 
# MAGIC The safest way to identify the culprit is to iteratively add table/view definitions back into your DAG starting from your initial ingestion tables. You can simply comment out later table/view definitions and uncomment these between runs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
