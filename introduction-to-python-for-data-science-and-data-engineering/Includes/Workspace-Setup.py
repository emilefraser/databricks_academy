# Databricks notebook source
# MAGIC %md <i18n value="1fb32f72-2ccc-4206-98d9-907287fc3262"/>
# MAGIC 
# MAGIC # Workspace Setup
# MAGIC This notebook should be run by instructors to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy All-Purpose Policy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs-Only Policy** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT-Only Policy** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create the Instance Pool **DBAcademy Pool** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Start a timer so we can benchmark execution duration.
setup_start = dbgems.clock_start()

# COMMAND ----------

# MAGIC %md <i18n value="86c0a995-1251-473e-976c-ba8288c0b2d3"/>
# MAGIC # Get Class Config
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

from dbacademy.dbhelper import WorkspaceHelper

# Setup the widgets to collect required parameters.
dbutils.widgets.dropdown(WorkspaceHelper.PARAM_CONFIGURE_FOR, "", 
                         WorkspaceHelper.CONFIGURE_FOR_OPTIONS, "Configure For (required)")

# lab_id is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "", "Lab/Class ID (optional)")

# a general purpose description of the class
dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "", "Description (optional)")

# COMMAND ----------

# MAGIC %md <i18n value="b1d39e1d-aa44-4c05-b378-837a1b432128"/>
# MAGIC 
# MAGIC # Init Script & Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.
# MAGIC 
# MAGIC It has the side effect of create our DA object which includes our REST client.

# COMMAND ----------

lesson_config.create_schema = False                 # We don't need a schema when configuring the workspace

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student

# COMMAND ----------

# MAGIC %md <i18n value="485ff12c-7286-4d14-a90e-3c29d87f8920"/>
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

instance_pool_id = DA.workspace.clusters.create_instance_pool()

# COMMAND ----------

# MAGIC %md <i18n value="04ae9a73-8b48-4823-8738-31e337864cf4"/>
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

from dbacademy.dbhelper import ClustersHelper

ClustersHelper.create_all_purpose_policy(client=DA.client, 
                                         instance_pool_id=instance_pool_id, 
                                         spark_version=None,
                                         autotermination_minutes_max=180,
                                         autotermination_minutes_default=120)

ClustersHelper.create_jobs_policy(client=DA.client, 
                                  instance_pool_id=instance_pool_id, 
                                  spark_version=None)

ClustersHelper.create_dlt_policy(client=DA.client, 
                                 lab_id=WorkspaceHelper.get_lab_id(), 
                                 workspace_description=WorkspaceHelper.get_workspace_description(),
                                 workspace_name=WorkspaceHelper.get_workspace_name(), 
                                 org_id=dbgems.get_org_id())

# COMMAND ----------

# MAGIC %md <i18n value="2f010a4b-af3c-4b3f-96a0-d8b3556ec728"/>
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC This task simply adds the "**databricks-sql-access**" entitlement to the "**users**" group ensuring that they can access the Databricks SQL view.

# COMMAND ----------

WorkspaceHelper.add_entitlement_workspace_access(client=DA.client)
# WorkspaceHelper.add_entitlement_databricks_sql_access(client=DA.client)

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")

