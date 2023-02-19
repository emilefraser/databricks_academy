# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="d6c49d6f-2235-4e65-b012-70bf6850330c"/>
# MAGIC 
# MAGIC 
# MAGIC # Data Visualization Lab
# MAGIC 
# MAGIC Let's import **`pandas`** and **`seaborn`**, then load the avocado dataset to perform data visualizations/analyses.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd
import seaborn as sns

# Set seaborn plot size to be easier to read
sns.set(rc = {"figure.figsize": (15,8)})

# COMMAND ----------

file_path = f"{DA.paths.datasets}/avocado/avocado.csv".replace("dbfs:", "/dbfs")
# Dropping incorrect index column.
df = pd.read_csv(file_path).drop("Unnamed: 0", axis=1) 
df

# COMMAND ----------

# MAGIC %md <i18n value="faba5cfc-f9cd-4012-845c-117fb3f184fa"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 1: Databricks Plotting
# MAGIC 
# MAGIC Using the built-in plotting feature in Databricks to plot the average **`Total Volume`** per **`year`**.
# MAGIC 
# MAGIC Remember to use **`display(df)`** to access built-in plotting.

# COMMAND ----------

# ANSWER
display(df)

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="4c4c04ec-8c7c-4dfe-b680-fc6e5e7a254f"/>
# MAGIC 
# MAGIC 
# MAGIC <button onclick="myFunction2()" >Click for Hint</button>
# MAGIC 
# MAGIC <div id="myDIV2" style="display: none;">
# MAGIC   Select the bar chart, Plot Options, set the aggregate function to average, and put year as key and Total Volume as value. 
# MAGIC </div>
# MAGIC <script>
# MAGIC function myFunction2() {
# MAGIC   var x = document.getElementById("myDIV2");
# MAGIC   if (x.style.display === "none") {
# MAGIC     x.style.display = "block";
# MAGIC   } else {
# MAGIC     x.style.display = "none";
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md <i18n value="0ee316c5-f8b7-441c-9326-05eb0e4fac6e"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 2: `pandas` plotting
# MAGIC 
# MAGIC Create a histogram of the **`AveragePrice`** of avocados using the pandas **`.hist()`** method.

# COMMAND ----------

# ANSWER
df["AveragePrice"].hist()

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="da21fa24-e730-4641-ace0-100da86d2c4f"/>
# MAGIC 
# MAGIC 
# MAGIC <button onclick="myFunction2()" >Click for Hint</button>
# MAGIC 
# MAGIC <div id="myDIV2" style="display: none;">
# MAGIC   Select the column to make a Series. Then call .hist() on the Series for the column.
# MAGIC </div>
# MAGIC <script>
# MAGIC function myFunction2() {
# MAGIC   var x = document.getElementById("myDIV2");
# MAGIC   if (x.style.display === "none") {
# MAGIC     x.style.display = "block";
# MAGIC   } else {
# MAGIC     x.style.display = "none";
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md <i18n value="00f37c99-64bb-4b48-b73f-f5b69310645a"/>
# MAGIC 
# MAGIC 
# MAGIC ## Datetime
# MAGIC 
# MAGIC Unfortunately, our **`Date`** column is represented as an object type, when we want it to be a **`datetime`** type so we can do operations based on time (e.g. plot in chronological order instead of lexicographical order).  
# MAGIC 
# MAGIC Luckily, `pandas` provides a function called [to_datetime()](https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html?highlight=to_datetime#pandas.to_datetime) that takes in a Series and converts the type to **`datetime`**.

# COMMAND ----------

# Notice the dtype of Date
df.dtypes

# COMMAND ----------

df["Date"] = pd.to_datetime(df["Date"])
df.dtypes

# COMMAND ----------

# MAGIC %md <i18n value="40b4972b-d755-4c4f-ac64-02a57ca8e9f2"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 3: `seaborn` plotting
# MAGIC 
# MAGIC Using **`seaborn`**, which is aliased as **`sns`** from above, create a scatter plot for the Total Volume of organic avocado sales over time for all of the US (e.g. filter on **`region`** for the **`TotalUS`** region & on **`type`** for just **`organic`**). Select **`Date`** as the x-axis.

# COMMAND ----------

# ANSWER
plot_df = df[(df["region"] == "TotalUS") & (df["type"] == "organic")]
sns.scatterplot(data=plot_df, x="Date", y="Total Volume")

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="47287cb5-ea6b-43bd-ac47-f84d5355a1a3"/>
# MAGIC 
# MAGIC 
# MAGIC <button onclick="myFunction2()" >Click for Hint</button>
# MAGIC 
# MAGIC <div id="myDIV2" style="display: none;">
# MAGIC   Recall the seaborn plot function looks like this: sns.scatterplot(data=, x=, y=). Create a filtered DataFrame to pass to this with the region and type we want.
# MAGIC </div>
# MAGIC <script>
# MAGIC function myFunction2() {
# MAGIC   var x = document.getElementById("myDIV2");
# MAGIC   if (x.style.display === "none") {
# MAGIC     x.style.display = "block";
# MAGIC   } else {
# MAGIC     x.style.display = "none";
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md <i18n value="4fd56628-9c66-44a8-9b2d-bad0552fb619"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 4: What about conventional avocados?
# MAGIC 
# MAGIC Create the same scatter plot except with conventional avocados instead of organic ones. What differences do you notice between the two? Notice the axis scales.

# COMMAND ----------

# ANSWER
d = df[(df["region"] == "TotalUS") & (df["type"] == "conventional")]
sns.scatterplot(data=d, x="Date", y="Total Volume")

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="2e5c4b7b-0b50-41f0-8936-e407defce6dd"/>
# MAGIC 
# MAGIC 
# MAGIC <button onclick="myFunction2()" >Click for Hint</button>
# MAGIC 
# MAGIC <div id="myDIV2" style="display: none;">
# MAGIC   Use the same code as before but make sure to filter with df["type"] == "conventional" this time
# MAGIC </div>
# MAGIC <script>
# MAGIC function myFunction2() {
# MAGIC   var x = document.getElementById("myDIV2");
# MAGIC   if (x.style.display === "none") {
# MAGIC     x.style.display = "block";
# MAGIC   } else {
# MAGIC     x.style.display = "none";
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
