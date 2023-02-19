# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="1736b729-f36e-45fe-9340-bef22ff58167"/>
# MAGIC 
# MAGIC 
# MAGIC # Advanced Pandas Lab
# MAGIC 
# MAGIC In this lab, we will answer the question: What regions in the US has the highest average total volume of organic avocado sales in 2018?

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md <i18n value="e460c1e8-54fb-4175-9482-ac1ee5be0945"/>
# MAGIC 
# MAGIC 
# MAGIC First, let's import pandas

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md <i18n value="77f86875-8f35-4c68-a04c-1f99482cc0ce"/>
# MAGIC 
# MAGIC 
# MAGIC ## Read CSV
# MAGIC 
# MAGIC To determine the regions with the highest total volume of average organic avocado sales, we will use the [avocado prices](https://www.kaggle.com/datasets/neuromusic/avocado-prices) dataset. 
# MAGIC 
# MAGIC We have provided the code to read in the data.

# COMMAND ----------

file_path = f"{DA.paths.datasets}/avocado/avocado.csv".replace("dbfs:", "/dbfs")
df = pd.read_csv(file_path).drop("Unnamed: 0", axis=1) # drop unnamed index column from data

# COMMAND ----------

# MAGIC %md <i18n value="c6d6129e-db7a-41e5-a14d-077dc501de1d"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 1: Data Analysis
# MAGIC 
# MAGIC Now that you have the DataFrame, you're ready to start doing some analysis. Remember the goal is to determine which regions of the US had the highest average total volume of organic avocado sales in 2018. 
# MAGIC 
# MAGIC Filter for on the **`type`** and **`year`** to find organic sales in 2018, and assign the result to **`filtered_df`**.

# COMMAND ----------

# TODO
filtered_df = TODO
filtered_df

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="bf6590ba-46e2-4cf7-aecb-405818023d33"/>
# MAGIC 
# MAGIC 
# MAGIC <button onclick="myFunction2()" >Click for Hint</button>
# MAGIC 
# MAGIC <div id="myDIV2" style="display: none;">
# MAGIC   Remember to use & for the and operator when creating the boolean array. df[(bool_array) & (bool_array)]
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

assert filtered_df.shape == (648, 13), "There are not the correct number of rows or columns"
assert filtered_df["year"].min() == 2018, "Only look at 2018 data"
assert len(filtered_df[filtered_df["type"] == "conventional"]) == 0, "There should be no rows about non-organic avocado sales"
print("Test passed!")

# COMMAND ----------

# MAGIC %md <i18n value="7555f96b-93e7-414a-9064-b0827ecaf1e6"/>
# MAGIC 
# MAGIC 
# MAGIC ## Problem 2: Find the Regions
# MAGIC 
# MAGIC Now, use **`filtered_df`** to determine the top 10 region with the highest average total volume of organic avocado sales in 2018. 
# MAGIC * To do this, create a DataFrame using **`filtered_df`** where each row consists of a **`region`** and the mean **`Total Volume`** of organic avocados for that region. 
# MAGIC * Sort that DataFrame in descending order, and keep the first 10 rows 
# MAGIC * Assign the output to the **`final_df`** variable

# COMMAND ----------

# TODO
final_df = TODO
final_df

# COMMAND ----------

testing_df = final_df.reset_index() if final_df.index.name == "region" else final_df
assert type(testing_df) == pd.DataFrame, "final_df should be a DataFrame, make sure it is not a Series. If it is a Series, make sure to use [['Total Volume']] instead of ['Total Volume']"
assert (testing_df.columns == ["region", "Total Volume"]).all(), "The only columns should be region and Total Volume. Make sure to use reset_index and that region is not currently the index column"
assert len(testing_df) == 10, "Only return the top 10 rows"
assert testing_df.iloc[0].values[0] == "TotalUS", "TotalUS should be the on the top row"
assert round(testing_df.iloc[0].values[1], 2) == 1510487.83, "TotalUS should have an average Total Volume of around 1510488"
assert testing_df.iloc[9].values[0] == "LosAngeles", "LosAngeles should be the on the 10th row"
assert round(testing_df.iloc[9].values[1], 2) == 102628.31, "LosAngeles should have an average Total Volume of around 102628"
print("Test passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
