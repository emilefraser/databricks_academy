# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Basic Setup

# COMMAND ----------

# MAGIC %run ../Includes/Common

# COMMAND ----------

import json
import requests

dbutils.widgets.text("pipeline_id", "", "Pipeline ID")
pipeline_id = dbutils.widgets.get("pipeline_id")
assert len(pipeline_id) > 0, "Please specify the Pipeline ID"

# COMMAND ----------

# MAGIC %md # Get the Existing Pipeline

# COMMAND ----------

pipeline = client.pipelines().get_by_id(pipeline_id)

# Print the current spec
print(json.dumps(pipeline.get("spec"), indent=4))

print("="*80)

# Print the full pipeline
print(json.dumps(pipeline, indent=4))

# COMMAND ----------

json_res = pipeline.get("spec")

# username = spark.sql("SELECT current_user()").first()[0]
username = "jacob.parr@databricks.com"

json_res['storage'].replace(username, "{username}")

# COMMAND ----------

def parse_library_path(json_res, username=None, source=False):
    if source is True:
        username = "Instructor-Led Source"
    new_paths = []
    for library_item in json_res["libraries"]:
        curr_path = library_item["notebook"]["path"]
        new_paths.append(curr_path.replace(username, "{username}"))
        
    return new_paths

# COMMAND ----------

new_paths = parse_library_path(json_res, username, source=True)

new_paths

# COMMAND ----------

import re

def parse_database_name(json_res, username):
    formatted_username = re.sub("[^a-zA-Z0-9]", "_", username)
    return json_res["target"].replace(formatted_username, "{clean_name}")

cleaned_target = parse_database_name(json_res, username)

cleaned_target

# COMMAND ----------

# MAGIC %md # Create Direct Call

# COMMAND ----------

new_pipeline = client.pipelines().create(
    name = "Testing 123/Direct", 
    storage = "dbfs:/user/jacob.parr@databricks.com/dbacademy/dewd/dlt_demo_81/storage", 
    target = "dbacademy_jacob_parr_databricks_com_dewd_dlt_demo_81", 
    notebooks = ["/Repos/Instructor-Led Source/data-engineering-with-databricks-source/Source/08 - Delta Live Tables/DE 8.1 DLT/DE 8.1.2 - SQL for Delta Live Tables"])

# Print the create params
print(json.dumps(new_pipeline, indent=4))

# COMMAND ----------

# Delete the pipeline just created
client.pipelines().delete_by_id(new_pipeline["pipeline_id"])

# COMMAND ----------

# MAGIC %md # Convert To Pipeline Params
# MAGIC Convert the existing query to a set of creation parameters

# COMMAND ----------

params = client.pipelines().existing_to_create(pipeline)

# Print the create params
print(json.dumps(params, indent=4))

# COMMAND ----------

# MAGIC %md # Create w/REST Client
# MAGIC The following demonstrates how to create the Pipeline using the DBAcademy REST client. 
# MAGIC 
# MAGIC This is used often with our tooling but is not always distributed with courseware.

# COMMAND ----------

# Alter as desired
# In general, all 4 parameters would be modified due to the fact that they are user specific
# In this example, we are actually only chaning the name for simplicity of the demo.
params["name"] = "Testing 123 w/Client"
params["storage"] = "dbfs:/user/jacob.parr@databricks.com/dbacademy/dewd/dlt_demo_81/storage"
params["target"] = "dbacademy_jacob_parr_databricks_com_dewd_dlt_demo_81"
params["libraries"][0]["notebook"]["path"] = "/Repos/Instructor-Led Source/data-engineering-with-databricks-source/Source/08 - Delta Live Tables/DE 8.1 DLT/DE 8.1.2 - SQL for Delta Live Tables"

# Create the pipeline using the DBAcademy client
new_pipeline = client.pipelines().create_from_dict(params)

# Print the create query
print(json.dumps(new_pipeline, indent=4))

# COMMAND ----------

# Delete the pipeline just created
client.pipelines().delete_by_id(new_pipeline["pipeline_id"])

# COMMAND ----------

# MAGIC %md # Create w/Requests Library
# MAGIC The following demonstrates how to create the Pipeline using the standard **`requests`** library.
# MAGIC 
# MAGIC This library is available in notebooks is easily included in courseware.

# COMMAND ----------

api_endpoint = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# Alter as desired
# In general, all 4 parameters would be modified due to the fact that they are user specific
# In this example, we are actually only chaning the name for simplicity of the demo.
params["name"] = "Testing 123 w/Requests Library"
params["storage"] = "dbfs:/user/jacob.parr@databricks.com/dbacademy/dewd/dlt_demo_81/storage"
params["target"] = "dbacademy_jacob_parr_databricks_com_dewd_dlt_demo_81"
params["libraries"][0]["notebook"]["path"] = "/Repos/Instructor-Led Source/data-engineering-with-databricks-source/Source/08 - Delta Live Tables/DE 8.1 DLT/DE 8.1.2 - SQL for Delta Live Tables"

response = requests.post(url = f"{api_endpoint}/api/2.0/pipelines", 
                         headers={"Authorization": f"Bearer {api_token}"}, 
                         data=json.dumps(params))

assert response.status_code in [200], f"({response.status_code}): {response.text}"

new_query = response.json()

# Print the create query
print(json.dumps(new_query, indent=4))

# COMMAND ----------

# Delete the query just created
client.pipelines().delete_by_id(new_query["pipeline_id"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
