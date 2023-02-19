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

dbutils.widgets.text("query_id", "", "Query ID")
query_id = dbutils.widgets.get("query_id")
assert len(query_id) > 0, "Please specify the Query ID"

# COMMAND ----------

# MAGIC %md # Get the Existing Query

# COMMAND ----------

query = client.sql().queries().get_by_id(query_id)

# Print the current query
print(json.dumps(query, indent=4))

# COMMAND ----------

# MAGIC %md # Convert Query to Params
# MAGIC Convert the existing query to a set of creation parameters

# COMMAND ----------

params = client.sql().queries().existing_to_create(query)

# Print the create query
print(json.dumps(params, indent=4))

# COMMAND ----------

# MAGIC %md # Create w/REST Client
# MAGIC The following demonstrates how to create the Databricks SQL Query using the DBAcademy REST client. 
# MAGIC 
# MAGIC This is used often with our tooling but is not always distributed with courseware.

# COMMAND ----------

# Alter as desired
params["name"] = "Testing 123 w/Client"

# Create the query using the DBAcademy client
new_query = client.sql().queries().create_from_dict(params)

# Print the create query
print(json.dumps(new_query, indent=4))

# COMMAND ----------

# Delete the query just created
client.sql().queries().delete_by_id(new_query["id"])

# COMMAND ----------

# MAGIC %md # Create w/Requests Library
# MAGIC The following demonstrates how to create the Databricks SQL Query using the standard **`requests`** library.
# MAGIC 
# MAGIC This library is available in notebooks is easily included in courseware.

# COMMAND ----------

api_endpoint = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# Alter as desired
params["name"] = "Testing 123 w/Requests Library"

response = requests.post(url = f"{api_endpoint}/api/2.0/preview/sql/queries", 
                         headers={"Authorization": f"Bearer {api_token}"}, 
                         data=json.dumps(params))

assert response.status_code in [200], f"({response.status_code}): {response.text}"

new_query = response.json()

# Print the create query
print(json.dumps(new_query, indent=4))

# COMMAND ----------

# Delete the query just created
client.sql().queries().delete_by_id(new_query["id"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
