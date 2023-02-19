# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage Users
# MAGIC The purpose of this notebook is to create and delete the specified list of users
# MAGIC 
# MAGIC Known usages include:
# MAGIC * None

# COMMAND ----------

# MAGIC %md ## Initialize Notebook
# MAGIC Run the following two cells to initialize this notebook.
# MAGIC 
# MAGIC Once initialized, select your options from the widgets above.

# COMMAND ----------

# MAGIC %run ../Includes/Common

# COMMAND ----------

init_usernames()

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: List Users
# MAGIC List the users currently in the workspace

# COMMAND ----------

users = client.scim().users().list()

print("Found the following users in the workspace:")
for user in users:
    username = user.get("userName")
    print(f"  {username}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Delete Users
# MAGIC Delete the selected users

# COMMAND ----------

for username in get_usernames():
    client.scim().users().delete_by_username(username)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create Users
# MAGIC Create each user specified in the usernames list

# COMMAND ----------

new_usernames = [
    "change.me@example.com",
]

# COMMAND ----------

assert "change.me@example.com" not in new_usernames, "Please update the list of usernames before executing this task."

for username in new_usernames:
    user = client.scim().users().get_by_username(username)
    if user is None:
        try:
            print(f"Creating the user {username}")
            client.scim().users().create(username)
        except Exception as e:
            print(e)
    else:
        print(f"Skipping creation of the user {username}")
        
# Update the current set of users
init_usernames()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
