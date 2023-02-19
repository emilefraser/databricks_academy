-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create external tables in Unity Catalog
-- MAGIC 
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Create a storage credential and external location
-- MAGIC * Control access to the external location
-- MAGIC * Create an external table using the external location
-- MAGIC * Manage access control to files using external locations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC 
-- MAGIC Run the following cells to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named database exclusively for your use. Additionally it will populate that database with a source table to be used later in the exercise.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: this notebook assumes a catalog named *main* in your Unity Catalog metastore. If you need to target a different catalog, edit the following notebook, **Classroom-Setup**, before proceeding.

-- COMMAND ----------

-- MAGIC %run ../../Includes/Classroom-Setup-06.99.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Storage Credential
-- MAGIC 
-- MAGIC The first prerequisite for creating external tables is to establish a credential to access the cloud file store where the table data will live. In Unity Catalog, this construct is referred to a **storage credential** and we will create one now.
-- MAGIC 
-- MAGIC We need a few pieces of information to create a storage credential.
-- MAGIC 
-- MAGIC * For Azure, we require a directory and application ID, and a client secret of a service principal that has been granted the **Azure Blob Contributor** role on the storage location. Refer to <a href="https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables#create-an-external-table" target="_blank">this document</a> for more details.
-- MAGIC * For AWS, we must define an IAM role authorizing access to the S3 bucket, and we will need the ARN for that role. Refer to <a href="https://docs.databricks.com/data-governance/unity-catalog/create-tables.html#create-an-external-table" target="_blank">this document</a> for more details.
-- MAGIC 
-- MAGIC In this video, we have already defined such a role using the well-documented procedure referenced above, and we have an ARN ready to go.
-- MAGIC 
-- MAGIC With that needed information, let's go to the **SQL** persona and, continuing to follow the relevant document linked above, let's create the storage credential. After creating the credential, take note of the name you assigned, and return to this notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To see the properties of the storage credential, use SQL as follows. Uncomment the following cell and substitute the name you assigned to your storage credential.

-- COMMAND ----------

-- DESCRIBE STORAGE CREDENTIAL <storage credential name>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While you can use storage credentials directly to manage access to external data, Databricks recommends instead using **external locations**, which contains a full path definition that references the appropriate storage credential. Using external locations in your governance strategy rather than storage credentials provides a finer degree of control over your cloud storage. Let's create one next.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create External Location
-- MAGIC 
-- MAGIC While we can use storage credentials directly to manage access to our external resources, Unity Catalog also provides a wrapper known as an **external location** that additionally specifies a path within the storage container. Using external locations for access control is preferred approach, since it gives us control at the file path level rather than at the level of the storage container itself.
-- MAGIC 
-- MAGIC In order to define an external location, we need some additional information.
-- MAGIC 
-- MAGIC * For AWS, we require the full S3 URL. This includes the bucket name and path within the container. Refer to <a href="https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#create-an-external-location" target="_blank">this document</a> for more details.
-- MAGIC * For Azure, we require the storage container path. Refer to <a href="https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables#create-an-external-location" target="_blank">this document</a> for more details.
-- MAGIC 
-- MAGIC Let's go back to the **SQL** persona and, continuing to follow the relevant document linked above, let's create the external location. If desired, you can assign your external location with the same name as the storage credential, though this is not generally good practice since there will usually be many external locations referencing a storage credential. In any case, take note of the name you assigned, and return to this notebook.
-- MAGIC 
-- MAGIC Following the appropriate guide based on your cloud provider, create the external location in the **Data** page of the **SQL** persona, then return to this notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To see the properties of the external location, use SQL as follows. Uncomment the following cell and substitute the name you assigned to your external location.

-- COMMAND ----------

-- DESCRIBE EXTERNAL LOCATION <external location name>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note the **url** value from the output of the previous cell. We will need that to create our external table next.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create External Table
-- MAGIC 
-- MAGIC With an external location in place, let's use it to create an external table. For the purpose of this example, we will use a **CREATE TABLE AS SELECT** statement to take a copy of the **silver_managed** table that was created as part of the setup.
-- MAGIC 
-- MAGIC The syntax for creating an external table is identical to a managed table, but with the addition of a **LOCATION** specification that specifies the full cloud storage path containing the data files fo the table.
-- MAGIC 
-- MAGIC Since we are not using storage credentials to manage access to our storage, then this operation can only succeed if:
-- MAGIC 
-- MAGIC * we are the ownder of an external location object covering the specified location (which happens to be the case here), or
-- MAGIC * we have **CREATE_TABLE** privilege on the external location object covering the specified location
-- MAGIC 
-- MAGIC And, as with managed tables, we must of course also have **CREATE** privilege on the database, as well as **USAGE** on the database and catalog.
-- MAGIC 
-- MAGIC Uncomment the following cell and subsitute the value of **url** from above.

-- COMMAND ----------

-- CREATE OR REPLACE TABLE silver_external
-- LOCATION '<url value from above>'
-- AS SELECT * FROM silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Grant access to an external table [optional]
-- MAGIC 
-- MAGIC Once an external table is created, access control works the same way as it does for a managed table. Let's see this in action now.
-- MAGIC 
-- MAGIC Note that you can only perform this section if you followed along with the *Manage users and groups* exercise and created a Unity Catalog group named **analysts**.
-- MAGIC 
-- MAGIC Perform this section by uncommenting the code cells and running them in sequence. You will also be prompted to run some queries as a secondary user. To do this:
-- MAGIC 
-- MAGIC 1. Open a separate private browsing session and log in to Databricks SQL using the user id you created when performing *Manage users and groups*.
-- MAGIC 1. Create a SQL endpoint following the instructions in *Create SQL Endpoint in Unity Catalog*.
-- MAGIC 1. Prepare to enter queries as instructed below in that environment.
-- MAGIC 
-- MAGIC Let's run the following cell to output a query statement that reads the external table. Copy and paste the output into a new query within the Databricks SQL environment of your secondary user, and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.{DA.schema_name}.silver_external")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Naturally, this will fail without the appropriate grants in place:
-- MAGIC * **USAGE** on the catalog
-- MAGIC * **USAGE** on the database
-- MAGIC * **SELECT** on the table
-- MAGIC 
-- MAGIC Let's address this now with the following three **GRANT** statements. Uncomment and run the following cell.

-- COMMAND ----------

-- GRANT USAGE ON CATALOG `${da.catalog_name}` to `analysts`;
-- GRANT USAGE ON DATABASE `${da.schema_name}` TO `analysts`;
-- GRANT SELECT ON TABLE silver_external to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Re-run the query as your third-party user now. As expected, this will now succeed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Comparing managed and external tables
-- MAGIC 
-- MAGIC Let's do a high-level comparison of the two tables using **DESCRIBE EXTENDED**. Uncomment and run the following two cells.

-- COMMAND ----------

-- DESCRIBE EXTENDED silver_managed

-- COMMAND ----------

-- DESCRIBE EXTENDED silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For the most part, the tables are similar, but the key difference lies in the **Location**.
-- MAGIC 
-- MAGIC Now let's compare behaviour when we drop and recreate these tables. Before we do that, let's use **SHOW CREATE TABLE** to capture the commands needed to recreate these tables after we drop them. Uncomment and run the following two cells.

-- COMMAND ----------

-- SHOW CREATE TABLE silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's drop both tables. Uncomment and run the following cell.

-- COMMAND ----------

-- DROP TABLE silver_managed
-- DROP TABLE silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's copy and paste the **CREATE TABLE** expression from above to recreate the **silver_managed** table.

-- COMMAND ----------

-- paste createtab_stmt for silver_managed from above

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Uncomment and run the following cell to display the contents of the recreated **silver_managed** table.

-- COMMAND ----------

-- SELECT * FROM silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's recreate the **silver_external** table, specifying the same schema as **silver_managed** but with the same **`LOCATION`** that we used earlier to create this external table initially, then display the contents.

-- COMMAND ----------

-- CREATE TABLE silver_external (
--   device_id INT,
--   mrn STRING,
--   name STRING,
--   time TIMESTAMP,
--   heartrate DOUBLE
-- ) 
-- LOCATION '<url value from earlier>';

-- SELECT * FROM silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice this key difference in behavior. In the managed case, all table data was discarded when the table was dropped. In the external case, since Unity Catalog does not manage the data, it was retained by recreating the table using the same location.
-- MAGIC 
-- MAGIC Before proceeding to the next section, let's uncomment and run the following cell to perform a bit of cleanup by dropping the **silver_external** table. Note again that this will just drop the table; the data files backing the table will be retained.

-- COMMAND ----------

-- DROP TABLE silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant access to files
-- MAGIC 
-- MAGIC Earlier, we created a storage credential and external location to allow us to create an external table whose data files reside on an external cloud store.
-- MAGIC 
-- MAGIC Storage credentials and external locations support additional specialized privileges that allow us to govern access to the files stored in those locations. These privileges include:
-- MAGIC 
-- MAGIC * **READ FILES**: ability to directly read files stored in this location
-- MAGIC * **WRITE FILES**: ability to directly write files stored in this location
-- MAGIC * **CREATE TABLE**: ability to create a table based on files stored in this location
-- MAGIC 
-- MAGIC The following SQL statement shows us a list of the files in the specified location. Uncomment and execute the following cell, substituting int the **url** value used earlier to create the external table **silver_external**. 

-- COMMAND ----------

-- LIST '<url value from earlier>';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's perform a **`SELECT`** against the table repressented by these data files. Remember, the table wrapping these data files was dropped; now we are simply performing this operation against raw data files that used to back the **silver_external** table.

-- COMMAND ----------

-- SELECT * FROM delta.`<same url value as previous command>`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No additional privileges are required to do these last two operations since we are the owner of the external location object covering this path.
-- MAGIC 
-- MAGIC If you are following along as a secondary user in Databricks SQL, copy and paste the above as a new query in that that environment and run it.
-- MAGIC 
-- MAGIC This fails since the user does not have a **READ FILES** grant on the location. Let's fix this now by running the following command, substituting in the name of the external location created earlier.

-- COMMAND ----------

-- GRANT READ FILES ON EXTERNAL LOCATION <external location name> TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Now attempt the **`SELECT`** operation again in the Databricks SQL environment. Exercising your **READ FILES** grant, you will now be met with success.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the database and table that was used in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additionally, let's go back to the **SQL** persona (as the current user, not the secondary user) and remove the external location and storage credential we created earlier.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
