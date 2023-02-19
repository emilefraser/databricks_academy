# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="358d2c22-9d78-4888-a7ec-54b7d5f3db64"/>
# MAGIC 
# MAGIC 
# MAGIC # Just Enough Python for Databricks SQL Lab
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Review basic Python code and describe expected outcomes of code execution
# MAGIC * Reason through control flow statements in Python functions
# MAGIC * Add parameters to a SQL query by wrapping it in a Python function

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.3L

# COMMAND ----------

# MAGIC %md <i18n value="97cba873-1459-478f-831b-b52fc54265eb"/>
# MAGIC 
# MAGIC 
# MAGIC # Reviewing Python Basics
# MAGIC 
# MAGIC In the previous notebook, we briefly explored using **`spark.sql()`** to execute arbitrary SQL commands from Python.
# MAGIC 
# MAGIC Look at the following 3 cells. Before executing each cell, identify:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment
# MAGIC 
# MAGIC Then execute the cells, compare the results to your expectations, and see the explanations below.

# COMMAND ----------

course = "dewd"

# COMMAND ----------

spark.sql(f"SELECT '{course}' AS course_name")

# COMMAND ----------

df = spark.sql(f"SELECT '{course}' AS course_name")
display(df)

# COMMAND ----------

# MAGIC %md <i18n value="bc8fda28-92ad-4cd5-aa24-34022269698a"/>
# MAGIC 
# MAGIC 
# MAGIC 1. **Cmd 5** assigns a string to a variable. When a variable assignment is successful, no output is displayed to the notebook. A new variable is added to the current execution environment.
# MAGIC 1. **Cmd 6** executes a SQL query and displays the schema for the DataFrame alongside the word **`DataFrame`**. In this case, the SQL query is just to select a string, so no changes to our environment occur. 
# MAGIC 1. **Cmd 7** executes the same SQL query and displays the output of the DataFrame. This combination of **`display()`** and **`spark.sql()`** most closely mirrors executing logic in a **`%sql`** cell; the results will always be printed in a formatted table, assuming results are returned by the query; some queries will instead manipulate tables or databases, in which case the word **`OK`** will print to show successful execution. In this case, no changes to our environment occur from running this code.

# COMMAND ----------

# MAGIC %md <i18n value="ef0b350e-c470-4e89-9617-948e49dd1710"/>
# MAGIC 
# MAGIC 
# MAGIC ## Setting Up a Development Environment
# MAGIC 
# MAGIC Throughout this course, we use logic similar to the following cell to capture information about the user currently executing the notebook and create an isolated development database.
# MAGIC 
# MAGIC The **`re`** library is the <a href="https://docs.python.org/3/library/re.html" target="_blank">standard Python library for regex</a>.
# MAGIC 
# MAGIC Databricks SQL has a special method to capture the username of the **`current_user()`**; and the **`.first()[0]`** code is a quick hack to capture the first row of the first column of a query executed with **`spark.sql()`** (in this case, we do this safely knowing that there will only be 1 row and 1 column).
# MAGIC 
# MAGIC All other logic below is just string formatting.

# COMMAND ----------

import re

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
schema_name = f"dbacademy_{clean_username}_{course}_5_3l"
working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

print(f"username:    {username}")
print(f"schema_name:     {schema_name}")
print(f"working_dir: {working_dir}")

# COMMAND ----------

# MAGIC %md <i18n value="1273f7a3-823a-4b1f-914a-ce6eaaa867b3"/>
# MAGIC 
# MAGIC 
# MAGIC Below, we add a simple control flow statement to this logic to create and use this user-specific database. 
# MAGIC 
# MAGIC Optionally, we will reset this database and drop all of the contents on repeat execution. (Note the the default value for the parameter **`reset`** is **`True`**).

# COMMAND ----------

def create_database(course, reset=True):
    import re

    username = spark.sql("SELECT current_user()").first()[0]
    clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    schema_name = f"dbacademy_{clean_username}_{course}_5_3l"
    working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

    print(f"username:    {username}")
    print(f"schema_name: {schema_name}")
    print(f"working_dir: {working_dir}")

    if reset:
        spark.sql(f"DROP DATABASE IF EXISTS {schema_name} CASCADE")
        dbutils.fs.rm(working_dir, True)
        
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name} LOCATION '{working_dir}/{schema_name}.db'")
    spark.sql(f"USE {schema_name}")
    
create_database(course)

# COMMAND ----------

# MAGIC %md <i18n value="cfa0adf3-cc23-4ba1-8daf-2c70af7fa079"/>
# MAGIC 
# MAGIC 
# MAGIC While this logic as defined is geared toward isolating students in shared workspaces for instructional purposes, the same basic design could be leveraged for testing new logic in an isolated environment before pushing to production.

# COMMAND ----------

# MAGIC %md <i18n value="1c994e19-2b72-45c3-a174-8a7e21701688"/>
# MAGIC 
# MAGIC 
# MAGIC ## Handling Errors Gracefully
# MAGIC 
# MAGIC Review the logic in the function below.
# MAGIC 
# MAGIC Note that we've just declared a new database that currently contains no tables.

# COMMAND ----------

def query_or_make_demo_table(table_name):
    try:
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Displayed results for the table {table_name}")
        
    except:
        spark.sql(f"CREATE TABLE {table_name} (id INT, name STRING, value DOUBLE, state STRING)")
        spark.sql(f"""INSERT INTO {table_name}
                      VALUES (1, "Yve", 1.0, "CA"),
                             (2, "Omar", 2.5, "NY"),
                             (3, "Elia", 3.3, "OH"),
                             (4, "Rebecca", 4.7, "TX"),
                             (5, "Ameena", 5.3, "CA"),
                             (6, "Ling", 6.6, "NY"),
                             (7, "Pedro", 7.1, "KY")""")
        
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Created the table {table_name}")

# COMMAND ----------

# MAGIC %md <i18n value="5a449d08-9811-4b0d-9004-74b8bb04eef5"/>
# MAGIC 
# MAGIC 
# MAGIC Try to identify the following before executing the next cell:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md <i18n value="8ddb2ea1-9e4e-4ac7-a369-ff984114653f"/>
# MAGIC 
# MAGIC 
# MAGIC Now answer the same three questions before running the same query below.

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md <i18n value="6efbda51-9c51-440a-aaaf-7276ad175398"/>
# MAGIC 
# MAGIC 
# MAGIC - On the first execution, the table **`demo_table`** did not yet exist. As such, the attempt to return the contents of the table created an error, which resulted in our **`except`** block of logic executing. This block:
# MAGIC   1. Created the table
# MAGIC   1. Inserted values
# MAGIC   1. Printed or displayed the contents of the table
# MAGIC - On the second execution, the table **`demo_table`** already exists, and so the first query in the **`try`** block executes without error. As a result, we just display the results of the query without modifying anything in our environment.

# COMMAND ----------

# MAGIC %md <i18n value="a0f957ea-7604-46b9-9b06-d672b73efcec"/>
# MAGIC 
# MAGIC 
# MAGIC ## Adapting SQL to Python
# MAGIC Let's consider the following SQL query against our demo table created above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, value 
# MAGIC FROM demo_table
# MAGIC WHERE state = "CA"

# COMMAND ----------

# MAGIC %md <i18n value="c4abcb35-3733-4565-8f8c-0df4b23f1e71"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC which can also be expressed using the PySpark API and the **`display`** function as seen here:

# COMMAND ----------

results = spark.sql("SELECT id, value FROM demo_table WHERE state = 'CA'")
display(results)

# COMMAND ----------

# MAGIC %md <i18n value="6a4e7e96-c53a-4b8e-abf5-412fe4170c27"/>
# MAGIC 
# MAGIC 
# MAGIC Let's use this simple example to practice creating a Python function that adds optional functionality.
# MAGIC 
# MAGIC Our target function will:
# MAGIC * Be based upon a query that only includes the **`id`** and **`value`** columns from the a table named **`demo_table`**
# MAGIC * Will allow filtering of that query by **`state`** where the the default behavior is to include all states
# MAGIC * Will optionally render the results of the query using the **`display`** function where the default behavior is to not render
# MAGIC * Will return:
# MAGIC   * The query result object (a PySpark DataFrame) if **`render_results`** is False
# MAGIC   * The **`None`** value  if **`render_results`** is True
# MAGIC 
# MAGIC Stretch Goal:
# MAGIC * Add an assert statement to verify that the value passed for the **`state`** parameter contains two, uppercase letters
# MAGIC 
# MAGIC Some starter logic has been provided below:

# COMMAND ----------

# TODO
def preview_values(state=<FILL-IN>, render_results=<FILL-IN>):
    query = <FILL-IN>

    if state is not None:
        <FILL-IN>

    if render_results
        <FILL-IN>


# COMMAND ----------

# MAGIC %md <i18n value="060207a1-a34c-4817-abee-f6e0b9c3b48a"/>
# MAGIC 
# MAGIC 
# MAGIC The assert statements below can be used to check whether or not your function works as intended.

# COMMAND ----------

import pyspark.sql.dataframe

assert type(preview_values()) == pyspark.sql.dataframe.DataFrame, "Function should return the results as a DataFrame"
assert preview_values().columns == ["id", "value"], "Query should only return **`id`** and **`value`** columns"

assert preview_values(render_results=True) is None, "Function should not return None when rendering"
assert preview_values(render_results=False) is not None, "Function should return DataFrame when not rendering"

assert preview_values(state=None).count() == 7, "Function should allow no state"
assert preview_values(state="NY").count() == 2, "Function should allow filtering by state"
assert preview_values(state="CA").count() == 2, "Function should allow filtering by state"
assert preview_values(state="OH").count() == 1, "Function should allow filtering by state"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
