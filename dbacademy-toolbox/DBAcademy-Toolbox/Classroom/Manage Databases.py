# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage Databases
# MAGIC The purpose of this notebook is to create and drop databases for each user in a workspace
# MAGIC 
# MAGIC Known usages include:
# MAGIC * Data Analysis with Databricks

# COMMAND ----------

# MAGIC %md ## Initialize Notebook
# MAGIC Run the following two cells to initialize this notebook.
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

# We don't want to accidently run the following tasks
# so we will be aborting any Run-All operations by exiting
dbutils.notebook.exit("Precluding Run-All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Create Course-Specific Databases
# MAGIC This task creates one database per user.

# COMMAND ----------

for username in get_usernames():
    db_name = to_db_name(get_course_code(), username)
    db_path = f"dbfs:/mnt/dbacademy-users/{username}/{get_course_code()}/database.db"
    
    print(f"Creating the database \"{db_name}\"\n   for \"{username}\" \n   at \"{db_path}\"\n")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")
    spark.sql(f"CREATE DATABASE {db_name} LOCATION '{db_path}';")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Grant Privileges
# MAGIC This tasks creates the Databricks SQL query which in turn is executed to grant each user access to their databases.

# COMMAND ----------

sql = ""
for username in get_usernames():
    db_name = to_db_name(get_course_code(), username)
    sql += f"GRANT ALL PRIVILEGES ON DATABASE `{db_name}` TO `{username}`;\n"
    sql += f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`;\n"
    sql += f"ALTER DATABASE {db_name} OWNER TO `{username}`;\n"    
    sql += "\n"
    
query_name = f"Instructor - Grant All Users - {get_course_name()}"
query = client.sql().queries().get_by_name(query_name)
if query is not None:
    client.sql().queries().delete_by_id(query.get("id"))

query = client.sql().queries().create(name=query_name, 
                                      query=sql[0:-1], 
                                      description="Grants the required access for all users to the databases for the course {course}",
                                      schedule=None, options=None, data_source_id=None)
query_id = query.get("id")
displayHTML(f"""Query created - follow this link to execute the grants in Databricks SQL</br></br>
                <a href="/sql/queries/{query_id}/source?o={dbgems.get_workspace_id()}" target="_blank">{query_name}</a>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Drop Course-Specific Databases
# MAGIC This task dropes each user's course-specific database.

# COMMAND ----------

for username in get_usernames():
    db_name = to_db_name(get_course_code(), username)
    
    print(f"Dropping the database \"{db_name}\"")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
