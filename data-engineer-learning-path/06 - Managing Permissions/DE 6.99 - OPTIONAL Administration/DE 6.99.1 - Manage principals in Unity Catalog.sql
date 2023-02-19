-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Manage principals in Unity Catalog
-- MAGIC 
-- MAGIC In this demo you will learn how to:
-- MAGIC * Create identities in the account console for users and service principals
-- MAGIC * Create groups and manage group memberships
-- MAGIC * Access identity management features

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC Databricks identities exist at two levels. Existing Databricks users are probably familiar with workspace-level identities, which were and continue to be linked to the credentials used to access Databricks services like the Data Science and Engineering Workspace, Databricks SQL, and Databricks Machine Learning. Prior to Unity Catalog, account-level identities had little relevance to most users, since these identities were used only to administer the Databricks account. With the introduction of Unity Catalog and its situation outside of the workspace, it made sense to root the identities it uses at the account level. Therefore, it's important to understand the distinction between these two levels of identity and how to manage the relationship between the two.
-- MAGIC 
-- MAGIC Account-level identities, which we focus on in this demo, are managed through the Databricks account console or its associated SCIM APIs. In this demo, we'll focus on account console usage, however we will take a look at the integration points for automating identity management.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC If you would like to follow along with this demo, you will need account administrator capabilities over your Databricks account, as creating identities is done in the account admininistrator console.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing users and service principals
-- MAGIC 
-- MAGIC In Databricks, a **user** corresponds to an individual interactively using the system – that is, a person. Users are identified through their email address, and authenticate using that email address and a password which they manage. Users interact with the platform through the user interface, and they can also access functionality through command-line tools and REST APIs. Best practice dictates that they generate a personal access token (PAT) for authenticating such tools.
-- MAGIC 
-- MAGIC Only those few users who are account administrators will actually log in to the account console. The rest will log in to one of the workspaces to which they are assigned. But, their identity at the account level is still critical in allowing them to access data through Unity Catalog.
-- MAGIC 
-- MAGIC A **service principal** is a different type of individual identity intended for use with automation tools and running jobs. These are assigned a name by an account administror, though they are identified through a globally unique identifier (GUID) that is dynamically generated when the identity is created. Service principals authenticate to a workspace using a token and access functionality through APIs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a user
-- MAGIC Let 's add a new user to our account.
-- MAGIC 
-- MAGIC 1. Log in to the <a href="https://accounts.cloud.databricks.com/" target="_blank">account console</a> as an account administrator.
-- MAGIC 1. In the left sidebar, let's click **User management**. The **Users** tab is shown by default.
-- MAGIC 1. Let's click **Add user**.
-- MAGIC 1. Provide an email address. This is the identifying piece of information that uniquely identifies users across the system. It must be a valid email, since that will be used to confirm identity and mangage their password. *For demo purposes, one can quickly create temporary email addresses using **dispostable.com.***
-- MAGIC 1. Provide a first and last name. Though these fields are not used by the system, they make identities more human-readable.
-- MAGIC 1. Click **Send invite**.
-- MAGIC 1. The new user will be issued an email inviting them to join and set their password.
-- MAGIC 
-- MAGIC Though this user has been added to the account, they will not be able to access Databricks services yet since they have not been assigned to any workspaces. However, their addition to the account does make them a valid principal in the eyes of Unity Catalog. Let's validate this now.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Validating user
-- MAGIC 
-- MAGIC Let's check to see that our newly created user can be seen by Unity Catalog. We can do this with **Data Explorer**, available in the Databricks SQL.
-- MAGIC 
-- MAGIC 1. From the account console, let's open one of the available workspaces.
-- MAGIC 1. Now let's switch to the **SQL** persona.
-- MAGIC 1. In the left sidebar, let's click **Data**.
-- MAGIC 1. In the **Data** pane, we see a list of *catalogs*. This gives us our first glipse of the three-level namespace in action. Select one of the catalogs, except for **hive_metastore** or **samples**. **main** is a usually good choice, since it is created by default with new metastores.
-- MAGIC 1. Select the **Permissions** tab.
-- MAGIC 1. Click **Grant**.
-- MAGIC 1. Perform a search by typing some element from the identity we just created (name, email address, domain name).
-- MAGIC 
-- MAGIC Notice how the identity appears in the dropdown list as a selectable entity. This mean that this identify is a valid principal as far Unity Catalog is concerned. It also means that we can start granting privileges on data objects to this user. However, this user can't actually do anything yet since they are not assigned to any workspaces.
-- MAGIC 
-- MAGIC **NOTE: Despite what was just said, it's recommended to manage permissions on data objects using groups, as we will discuss in more detail shortly. This practice leads to a system that is much more maintainable.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deleting a user
-- MAGIC 
-- MAGIC If a user leaves the organization we can delete them without affecting any data objects they own. If they are merely absent for an extended period but we wish to temporarily revoke access, then users can also be deactivated (though this option is only available through <a href="https://docs.databricks.com/dev-tools/api/latest/scim/scim-users.html#activate-and-deactivate-user-by-id" target="_blank">the API</a> at the present time).
-- MAGIC 
-- MAGIC Let's see how to delete a user:
-- MAGIC 1. From the **User management** page of the account console, locate and select the targeted user (using the **Search** field if desired).
-- MAGIC 1. Click the three dots at the top-right corner of the page and select **Delete user** (you will be prompted to confirm but for now you can cancel).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Service principals
-- MAGIC 
-- MAGIC The workflow for managing service principals is virtually identical to users. Let's see how to add one now.
-- MAGIC 1. From the **User management** page of the account console, select the **Service principals** tab.
-- MAGIC 1. Let's click **Add service principal**.
-- MAGIC 1. Let's provide a name. Though this isn't an identifying piece of information, it's helpful to use something that will make sense for administrators.
-- MAGIC 1. Click **Add**.
-- MAGIC 
-- MAGIC Service principals are identified by their **Application ID**.
-- MAGIC 
-- MAGIC To delete a service principal:
-- MAGIC 1. In the **Service Principals** tab, locate and select the desired service principal from the list, using the **Search** field if necessary.
-- MAGIC 1. Click the three dots at the top-right corner of the page and select **Delete** (you will be prompted to confirm but for now you can cancel).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Automating user management
-- MAGIC 
-- MAGIC Databricks supports a SCIM API and can integrate with an external identity provider to automate user and account creation. This integration carries some important benefits:
-- MAGIC * Relieves the account administrator of the responsibilities of managing users and groups
-- MAGIC * Simplifies synchronization. Though Databricks maintains its own identities for its users, the identity provider integration can automatically manage users on the admin's behalf
-- MAGIC * Simplifies the user experience by accessing Databricks through your organization's SSO
-- MAGIC 
-- MAGIC Setting this up is outside the scope of this training, however here is how you can access these features:
-- MAGIC 1. Let's click on the **Settings** icon in the left sidebar of the account console.
-- MAGIC 1. Refer to the **Single sign-on** and **User Provisioning** tabs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing groups
-- MAGIC 
-- MAGIC The concept of groups is pervasive to countless security models, and for good reason. Groups gather individual users (and service principals) into a logical units to simplify management. Groups can also be nested within other groups if needed. Any grants on the group are automatically inherited by all members of the group.
-- MAGIC 
-- MAGIC Data governance policies are generally defined in terms of roles, and groups provide a user management construct that nicely maps to such roles, simplifying the implementation of these governance policies. In this way, permissions can be granted to groups in accordance with your organization’s security policies, and users can be added to groups as per their roles within the organization.
-- MAGIC 
-- MAGIC When users transition between roles, it’s simple to move a user from one group to another. Performing an equivalent operation when permissions are hard-wired at the indivdual user level is significantly more intensive. Likewise, as your governance model evolves and role definitions change, it’s much easier to effect those changes on groups rather than having to replicate changes across a number of individual users.
-- MAGIC 
-- MAGIC For these reasons, we advise implementing groups and granting data permissions to groups rather than individual users or service principals.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a group
-- MAGIC 
-- MAGIC Let's add a new group.
-- MAGIC 
-- MAGIC 1. From the **User management** page of the account console, select the **Groups** tab.
-- MAGIC 1. Click **Add group**.
-- MAGIC 1. Let's give the group a name (for example *analysts*).
-- MAGIC 1. Finally, click **Save**.
-- MAGIC 
-- MAGIC Let's repeat the process to create another new group named *metastore_admins*.
-- MAGIC 
-- MAGIC From here we can immediately add members to the new group, or it's a task that can be done at any time.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing group members
-- MAGIC 
-- MAGIC Let's add the user we created earlier to the *analysts* group we just created.
-- MAGIC 
-- MAGIC 1. In the **Groups** tab, locate and select the *analysts* group, using the **Search** field if desired.
-- MAGIC 1. Click **Add members**.
-- MAGIC 1. From here we can use the searchable text field that also functions as a drop-down menu when clicked. Let's identify the users, service principals or groups we want to add (search for and select user created earlier), then click **Add**. Note that we can add multiple principals at once if needed.
-- MAGIC 
-- MAGIC The group membership takes effect immediately.
-- MAGIC 
-- MAGIC Now let's repeat the process to add ourselves (**not the created users**) to the *metastore_admins* group that we just created. The intent of this group is to simplify the management of metastore administrators, but this action won't actually do anything yet. We'll get to that in the next section.
-- MAGIC 
-- MAGIC Removing a principal from a group is a simple matter.
-- MAGIC 1. Locate and select the group you want to manage in the **Groups** tab.
-- MAGIC 1. Locate the principal, using the **Search** field if desired.
-- MAGIC 1. Click the three dots in the rightmost column.
-- MAGIC 1. Select **Remove** (you will be prompted to confirm but for now you can cancel).
-- MAGIC 
-- MAGIC All principals listed in the group (including child groups) automatically inherit any grants given to the group. Assigning privileges in a group-wise fashion like this is considered a data governance best practice since it greatly simplifies the implementation and maintenance of an organization's security model.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deleting a group
-- MAGIC 
-- MAGIC As your data governance model evolves, it may become necessary to eliminate groups. Deleting groups in Unity Catalog destroys the membership structure and the permissions it conveyed, but it won't recursively delete its members, nor will it affect permissions granted directly to those individuals or child groups.
-- MAGIC 
-- MAGIC In the left sidebar, let's click **Users & Groups**.
-- MAGIC 1. In the **Groups** tab, locate the targeted group, using the **Search** field if desired.
-- MAGIC 1. Click the three dots in the rightmost column.
-- MAGIC 1. Select **Delete** (you will be prompted to confirm but for now you can cancel).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing workspace principals
-- MAGIC 
-- MAGIC Account administrators can assign principals (users and service principals individually, or groups) to one or more workspaces. If a user is assigned to more than one workspace, the navigational sidebar on the left will present them with the ability to switch between workspaces they have access to.
-- MAGIC 
-- MAGIC 1. In the account console, let's click the **Workspaces** icon in the left sidebar.
-- MAGIC 1. Locate and select the targeted workspace, using the **Search** field if desired.
-- MAGIC 1. Select the **Permissions** tab.
-- MAGIC 1. A list of currently assigned principals is displayed. To add more, let's click **Add permissions**.
-- MAGIC 1. Search for a user, service principal, or group to assign (search for *analysts*). 
-- MAGIC 1. The **Permission** dropdown provides the option to add the principal as a regular user or a workspace administrator. Leave this set to *User*.
-- MAGIC 1. Specify additional principals, if desired.
-- MAGIC 1. When done, click **Save**.
-- MAGIC 
-- MAGIC Account administrators can similarly unassign users or service principals from a workspace.
-- MAGIC 1. In the **Workspaces** page, locate and select the targeted workspace, using the **Search** field if desired.
-- MAGIC 1. Select the **Permissions** tab.
-- MAGIC 1. Locate the desired principal, and click the three dots in the rightmost column.
-- MAGIC 1. Select **Remove** (you will be prompted to confirm but for now you can cancel).
-- MAGIC 
-- MAGIC NOTE: workspace administrators can administer users within the workspaces they administer, though it's considered best practice to manage principals at the account level.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
