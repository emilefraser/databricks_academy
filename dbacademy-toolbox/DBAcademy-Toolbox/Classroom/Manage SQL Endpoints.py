# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage SQL Endpoints
# MAGIC The purpose of this notebook is to create, start, stop and delete endpoints for each user in a workspace
# MAGIC 
# MAGIC Known usages include:
# MAGIC * Data Analysis with Databricks

# COMMAND ----------

# MAGIC %md ## Initialize Notebook
# MAGIC Run the following two cell to initialize this notebook.
# MAGIC 
# MAGIC Once initialized, select your options from the widgets above.

# COMMAND ----------

# MAGIC %run ../Includes/Common

# COMMAND ----------

init_course()
init_usernames()

# COMMAND ----------

# MAGIC %md ## Review Your Selections
# MAGIC Run the following cell to review your selections:

# COMMAND ----------

print(f"Course Code: {get_course_code()}")
print(f"Course Name: {get_course_name()}")

print("\nThis notebook's tasks will be applied to the following users:")
for username in get_usernames():
    print(f"  {username}")

# COMMAND ----------

dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create SQL Endpoints

# COMMAND ----------

client.sql().endpoints().create_user_endpoints(naming_template=naming_template,             # Required
                                               naming_params=get_naming_params(),           # Required
                                               cluster_size = CLUSTER_SIZE_2X_SMALL,        # Required
                                               enable_serverless_compute = False,           # Required
                                               tags = {                                     
                                                   "dbacademy.course": get_course_name(),   # Tag the name of the course
                                                   "dbacademy.source": "DBAcadmey Toolbox"  # Tag the name of the course
                                               },  
                                               users=get_usernames())                       # Restrict to the specified list of users
# See docs for more parameters
# print(client.sql().endpoints().create_user_endpoints.__doc__)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: List SQL Endpoints

# COMMAND ----------

print("Found the following endpoints:")
for endpoint in client.sql().endpoints().list():
    name = endpoint.get("name")
    size = endpoint.get("size")
    print(f"  {name}: {size}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: Start SQL Endpoint

# COMMAND ----------

client.sql().endpoints().start_user_endpoints(naming_template, get_naming_params(), get_usernames())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task: Stop SQL Endpoints

# COMMAND ----------

client.sql().endpoints().stop_user_endpoints(naming_template, get_naming_params(), get_usernames())

# COMMAND ----------

# MAGIC %md ## Task: Delete SQL Endpoints

# COMMAND ----------

client.sql().endpoints().delete_user_endpoints(naming_template, get_naming_params(), get_usernames())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
