# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Workspace Setup
# MAGIC 
# MAGIC Run this notebook before class to ensure all assets are ready.
# MAGIC 
# MAGIC The steps include:
# MAGIC 1. Configure User Permissions
# MAGIC 2. Install the Datasets to the Workspace
# MAGIC 3. Create SQL Warehouse
# MAGIC 4. Create User-Specific Databases
# MAGIC 5. Update User-Specific Grants
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> The execution duration of this notebook can vary between 5 and 15 minutes depending on region, vm availability and other uncontrollable factors.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

from dbacademy.dbhelper import WorkspaceHelper

# Setup the widgets to collect required parameters.
dbutils.widgets.dropdown(WorkspaceHelper.PARAM_CONFIGURE_FOR, "", 
                         WorkspaceHelper.CONFIGURE_FOR_OPTIONS, "Configure Workspace For")

# lab_id is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "", "Lab/Class ID")

# a general purpose description of the class
dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "", "Description")

# COMMAND ----------

# MAGIC %md ## Setup Steps
# MAGIC The following steps document how to configure the workspace for this class.
# MAGIC 
# MAGIC This includes adjusting permissions, installing datasets, creating SQL Warehouses and user-specific databases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Consider manually removing access to the **Data Science & Engineering** view to prevent students from opening the wrong view.  
# MAGIC This can be done by removing the **Workspace access** entitlement for the **users** group in the **Admin Console**.

# COMMAND ----------

DA.update_entitlements()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Install the Datasets to the Workspace
# MAGIC This task will copy data from our data repository into the workspace, creating a local copy that all students will share. 
# MAGIC 
# MAGIC This will be "installed" to **/mnt/dbacademy-datasets/data-analysis-with-databricks/v02**.
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> Set `reinstall=True` to force deletion and reinstallation of the datasets for this one course.

# COMMAND ----------

DA.install_datasets(reinstall_datasets=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create SQL Warehouse
# MAGIC This task will create one SQL Warehouse for the whole class named **`Starter Warehouse`**
# MAGIC 
# MAGIC Upon completion the warehouse will be "**Running**".
# MAGIC 
# MAGIC To complete this step, simply run the following command.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the warehouse is created hours-to-days before a class, all warehouses should be stopped so as not to consume unnecissary charges between the point of creation and first usage by students.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> If the warehouse already exists but is not started, it **will** be updated but those changes may not appear until started - the instructor will be reponsible for starting the warehouse.

# COMMAND ----------

from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper
DA.workspace.warehouses.create_shared_sql_warehouse(name=WarehousesHelper.WAREHOUSES_DEFAULT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create User-Specific Databases
# MAGIC 
# MAGIC This tasks will create one database per user and corresponding tables from the datasets installed to **/mnt/dbacademy-datasets/data-analysis-with-databricks/v02**.
# MAGIC 
# MAGIC Run the following cell to create each database with the prescribed tables.

# COMMAND ----------

DA.create_user_databases(drop_existing=False)

# COMMAND ----------

# MAGIC %md ### Step 5: Update User-Specific Grants
# MAGIC 
# MAGIC Grant each user access to their personal databases.
# MAGIC 
# MAGIC This requires creating a job using an HA cluster to update user permissions.
# MAGIC 
# MAGIC For reference, this job runs the notebook [Configure-Permissions]($./Configure-Permissions)
# MAGIC 
# MAGIC To complete this step, simply run the following command.

# COMMAND ----------

from dbacademy.dbhelper.databases_helper_class import DatabasesHelper

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
job_id = DatabasesHelper.configure_permissions(DA.client, "Configure-Permissions", "10.4.x-scala2.12")
DA.client.jobs().delete_by_id(job_id)

# COMMAND ----------

DA.setup_completed()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
