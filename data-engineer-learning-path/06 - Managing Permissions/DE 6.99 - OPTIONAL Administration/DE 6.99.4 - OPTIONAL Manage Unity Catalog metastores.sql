-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Manage Unity Catalog metastores
-- MAGIC 
-- MAGIC In this demo you will learn how to:
-- MAGIC * Create and delete metastores
-- MAGIC * Assign a metastore to a workspace
-- MAGIC * Configure metastore administrators following best practices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC A metastore is the top-level container of objects in the Unity Catalog security model and manages metadata and access control lists for your data objects.
-- MAGIC 
-- MAGIC Account administrators create metastores and assign them to workspaces to allow workloads in those workspaces to access the data represented in the metastore. This can be done in the account console, through REST APIs, or using <a href="https://registry.terraform.io/providers/databrickSlabs/databricks/latest/docs" target="_blank">Terraform</a>. In this demo, we will explore the creation and management of metastores interactively using the account console.
-- MAGIC 
-- MAGIC There are some underlying cloud resources that must be set up by your cloud administrator first in order to support the metastore. This includes:
-- MAGIC * A cloud storage container
-- MAGIC * A cloud credential that allows Databricks to access the container
-- MAGIC 
-- MAGIC Creation of these resources is outside the scope of this demo. Please refer to our documentation for more information:
-- MAGIC * <a href="https://docs.databricks.com/data-governance/unity-catalog/get-started.html#configure-aws-objects" target="_blank">AWS</a>
-- MAGIC * <a href="https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started" target="_blank">Azure</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC If you would like to follow along with this demo, you will need:
-- MAGIC * Account adminstrator capabilities over your Databricks account, as creating metastores is done in the account administrator console.
-- MAGIC * Cloud resources as outlined in the **Overview** section, provided by your cloud administrator.
-- MAGIC * Complete the procedures outlined in the *Managing principals in Unity Catalog* demo (specifically, you need a *metastore_admins* group)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a metastore
-- MAGIC 
-- MAGIC With our supporting resources in place, let's create a metastore.
-- MAGIC 
-- MAGIC 1. Log in to the <a href="https://accounts.cloud.databricks.com/" target="_blank">account console</a> as an account administrator.
-- MAGIC 1. In the left sidebar, let's click **Data**. This displays a list of currently created metastores.
-- MAGIC 1. Let's click **Create metastore**.
-- MAGIC 1. Let's provide a name for the metastore. Though the name must be unique for your account use whatever naming convention you desire. Users will not have any visibility into the metastore name.
-- MAGIC 1. The **Region** setting allows us to specify the region in which to host the metastore. Metadata is maintained in the control plane, and must align geographically with the workspaces to which this metastore will be assigned, so choose a region accordingly.
-- MAGIC     <br>**NOTE: there can only be one metastore per region.** If the desired region isn't available, it's possible that you already have a metastore created there. If this is the case, then use that metastore and follow best practices using catalogs to segregate your data according to your needs.
-- MAGIC 1. Specify the path in the cloud storage container, as provided by your cloud administrator. Managed table data files will be stored at this location. For AWS, this is an S3 bucket path. For Azure, this is an ADLS Gen 2 path.
-- MAGIC 1. Specify the credential for accessing the storage container, as provided by your cloud administrator. For AWS, this is an IAM role ARN. For Azure, this is an Access Connector ID.
-- MAGIC 1. Finally, let's click **Create**.
-- MAGIC 
-- MAGIC From here we can assign the newly created metastore to any of the workspaces available in this account. But for now let's click **Skip** as this can be done at any time in the future.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting a metastore
-- MAGIC 
-- MAGIC When metastores are no longer needed, we can delete them. Note that this operation only deletes the metadata associated with the metastore. You should verify that the bucket is cleared in order to expunge data from managed tables.
-- MAGIC 
-- MAGIC 1. In the **Data** page, locate and select the targeted metastore, using the **Search** field if desired.
-- MAGIC 1. Click the three dots in the top-right corner of the page and select **Delete** (you will be prompted to confirm but for now you can cancel).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing workspace assignments
-- MAGIC 
-- MAGIC In order for compute resources to access a metastore, it must be assigned to a workspace. Note the following rules related to metastore assignment:
-- MAGIC * A metastore must be located in the same region as the workspace
-- MAGIC * A metastore can be assigned to multiple workspaces (in the same region)
-- MAGIC * A workspace can only have one metastore assigned at any given time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Assigning a metastore to a workspace
-- MAGIC 
-- MAGIC Metastore assignment can be done at any time, although if shuffling assignments take care that no workloads are running that might be disturbed. Let's assign one now.
-- MAGIC 
-- MAGIC 1. In the **Data** page of the account console, let's select the metastore we want to assign (using the **Search** field if desired).
-- MAGIC 1. Select the **Workspaces** tab. This displays a list of workspaces to which the metastore is currently assigned.
-- MAGIC 1. Let's click **Assign to workspace**.
-- MAGIC 1. Let's select the desired workspace(s).
-- MAGIC 1. Finally, we click **Assign**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Removing a metastore from a workspace
-- MAGIC 
-- MAGIC If we need to swap out a metastore, the process is similar. We can detach the metastore from any currently assigned workspace.
-- MAGIC 1. In the **Data** page, let's select the metastore we want to unassign (using the **Search** field if desired).
-- MAGIC 1. Select the **Workspaces** tab. This displays a list of workspaces to which the metastore is currently assigned.
-- MAGIC 1. Locate the workspace using the **Search** field if desired, and click the three dots in the rightmost column.
-- MAGIC 1. Let's select **Remove from this metastore** (you will be prompted to confirm but for now you can cancel).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transferring metastore administration
-- MAGIC 
-- MAGIC By default, the account administrator who created a metastore is the administrator for the metastore. Metastore admin responsibilities are numerous, and include:
-- MAGIC * Creating and assigning permissions on catalogs
-- MAGIC * Creating and removing schemas, tables and views if other users are not permitted to do so under your data governance policies
-- MAGIC * Integrating external storage into the metastore
-- MAGIC * Setting up shares with Delta Sharing
-- MAGIC * General maintenance and administative tasks on data objects when their owners ares not accessible; for example, transferring ownership of a schema owned by a user who has left the organization
-- MAGIC 
-- MAGIC Account administrators are very likely already busy individuals. So to avoid bottlenecks in your data governance processes, it's important to grant metastore admin to a group so that anyone in the group can take on these tasks. As roles change, users can be easily added or removed from that group.
-- MAGIC 
-- MAGIC 1. In the **Data** page, select the targeted metastore (using the **Search** field, if desired).
-- MAGIC 1. Locate the **Owner** field, which displays the administrator. Currently, this happens to be the individual in the organization who originally created the metastore. 
-- MAGIC 1. Select the **Edit** link to the right of the **Owner**.
-- MAGIC 1. Choose a principal that will become the new administrator. Let's use the *metastore_admins* group.
-- MAGIC 1. Click **Save**.
-- MAGIC 
-- MAGIC Now, anyone in that group will have the ability to perform administrative tasks on the metastore.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
