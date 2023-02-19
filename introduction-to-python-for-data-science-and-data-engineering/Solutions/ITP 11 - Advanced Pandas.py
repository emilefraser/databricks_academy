# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="84a7d8c4-2962-4cfa-b71a-1eb0e9d6a637"/>
# MAGIC 
# MAGIC 
# MAGIC # Advanced Pandas
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
# MAGIC * Explore some more advanced features pandas provides including:
# MAGIC   - Renaming columns
# MAGIC   - Filtering the DataFrame
# MAGIC   - Grouping and aggregation Functions
# MAGIC   - Sorting
# MAGIC   - Imputing columns

# COMMAND ----------

# MAGIC %md <i18n value="4b32f906-0ced-49ca-aef9-d0ed007c298a"/>
# MAGIC 
# MAGIC 
# MAGIC For this lesson, we are going to work with datasets. Running the cell below will define and give us access to variables defining the path to our datasets in the Databricks File System.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md <i18n value="5bbc9d31-e52b-4cbd-9e16-6f48fb1c051f"/>
# MAGIC 
# MAGIC 
# MAGIC Remember, to access **`pandas`** functionality, we must **`import`** the library first. We do not have to **`pip install pandas`** because we already have it installed.

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md <i18n value="a6a56c30-96e2-40dd-a36b-57969142819e"/>
# MAGIC 
# MAGIC 
# MAGIC ## Reading Data
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/301/sf.jpg" style="height: 200px; margin: 10px; border: 1px solid #ddd; padding: 10px"/>
# MAGIC 
# MAGIC So far we have created a DataFrame by manually specifying the rows and columns. Often, we will have a dataset stored as a CSV (comma-separated-value) file. 
# MAGIC 
# MAGIC **`pandas`** provides a function called [**read_csv(path)**](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html), where we provide a path to where our CSV file is stored, and it returns a DataFrame of the contents at that path.
# MAGIC 
# MAGIC You'll be analyzing data from <a href="http://insideairbnb.com/get-the-data.html" target="_blank">Inside Airbnb</a> to better understand the San Francisco rental market. Let's read in the dataset.

# COMMAND ----------

file_path = f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv".replace("dbfs:", "/dbfs")
df = pd.read_csv(file_path)

# COMMAND ----------

# MAGIC %md <i18n value="005adbc3-7ac2-4097-b131-e13953b0f178"/>
# MAGIC 
# MAGIC 
# MAGIC To look at the first few records of the dataset, we can call [**head()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.head.html). If you do not specify the number of rows, it defaults to 5 rows.

# COMMAND ----------

df.head(3)

# COMMAND ----------

# MAGIC %md <i18n value="03959db2-4350-4824-8617-ac7a3cd8be51"/>
# MAGIC 
# MAGIC 
# MAGIC Conversely, we can call [**tail()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.tail.html) to look at the last few records.

# COMMAND ----------

df.tail(3)

# COMMAND ----------

# MAGIC %md <i18n value="f67fb620-5e6a-4576-808a-9f2e5c4a7ba3"/>
# MAGIC 
# MAGIC 
# MAGIC ## Renaming Columns
# MAGIC 
# MAGIC We can rename columns of our DataFrame using [**rename()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html). We pass into columns a dictionary containing the mappings from the old column names to the new ones to the **`columns`** parameter.
# MAGIC 
# MAGIC Let's rename the **`neighbourhood`** column above to be **`neighborhood`**.

# COMMAND ----------

df = df.rename(columns={"neighbourhood": "neighborhood"})
df[["id", "neighborhood"]].head(10)

# COMMAND ----------

# MAGIC %md <i18n value="ac2317e2-da36-4d53-9af2-e0a3a77e86d6"/>
# MAGIC 
# MAGIC 
# MAGIC ## Filtering
# MAGIC 
# MAGIC Often, you will want to select a subset of rows that meet a certain criteria, which can be accomplished by specifying: **`df[bool_array]`**, where **`bool_array`** is a **`Series`** of **`True`** and **`False`** values for each row. 
# MAGIC 
# MAGIC The rows that evaluate to **`True`** are kept, while the ones that evaluate to **`False`** are not. 
# MAGIC 
# MAGIC Let's filter for all the rows **`host_is_superhost`** is **`"t"`**, meaning the airbnb owner is a superhost.

# COMMAND ----------

filtered_df = df[df["host_is_superhost"] == "t"]
filtered_df[["id", "host_is_superhost"]].head(10)

# COMMAND ----------

# MAGIC %md <i18n value="2fc07a19-40ee-4c02-aef8-47f8d4d5320d"/>
# MAGIC 
# MAGIC 
# MAGIC Here, **`df["host_is_superhost"] == "t"]`** is our boolean array. Let's take a look at the corresponding **True/False** row indices.

# COMMAND ----------

df["host_is_superhost"] == "t"

# COMMAND ----------

# MAGIC %md <i18n value="5060945e-605f-4103-b37f-bcce30875d2e"/>
# MAGIC 
# MAGIC 
# MAGIC We can also search for all the records where the **`host_is_superhost`** is NOT "t".

# COMMAND ----------

df["host_is_superhost"] != "t"

# COMMAND ----------

# MAGIC %md <i18n value="4491d033-fe78-482a-9f84-e64ee89914b4"/>
# MAGIC 
# MAGIC  
# MAGIC ## Pandas Boolean Operators
# MAGIC 
# MAGIC Often you will want to evaluate multiple criteria to filter out records. For example, let's select all records where the host is a superhost and the airbnb has at least 150 reviews.
# MAGIC 
# MAGIC Instead of the normal Boolean operators we have seen previously, we have [bitwise Boolean operators](https://www.w3schools.com/python/gloss_python_bitwise_operators.asp):
# MAGIC * **`and`** -> **`&`**
# MAGIC * **`or`** -> **`|`** 
# MAGIC * **`not`** -> **`~`**

# COMMAND ----------

filtered_df = df[(df["host_is_superhost"] == "t") & (df["number_of_reviews"] >= 150)]
filtered_df[["id", "host_is_superhost", "number_of_reviews"]].head(10)

# COMMAND ----------

# MAGIC %md <i18n value="ab96aa56-71eb-41f6-86f5-9cb8694eb956"/>
# MAGIC 
# MAGIC  
# MAGIC ## Aggregate Functions
# MAGIC 
# MAGIC Aggregate functions are functions that take in a series of inputs and return a single output. 
# MAGIC 
# MAGIC The most common ones that we use in pandas are ones that take in numerical **`Series`** and return a statistic of interest, such as the mean. 
# MAGIC 
# MAGIC Let's take a look at the mean, min, and max of **`number_of_reviews`**:

# COMMAND ----------

print(df["number_of_reviews"].mean())
print(df["number_of_reviews"].min())
print(df["number_of_reviews"].max())

# COMMAND ----------

# MAGIC %md <i18n value="80541416-8c3c-4ed2-9c8c-d3345995c67d"/>
# MAGIC 
# MAGIC  
# MAGIC Another useful method is [**describe()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html) which provides a report of summary statistics on a given numerical **`Series`**:

# COMMAND ----------

df["number_of_reviews"].describe()

# COMMAND ----------

# MAGIC %md <i18n value="6479f736-b87d-412d-a422-6079e807f12a"/>
# MAGIC 
# MAGIC  
# MAGIC We can also use this method on a DataFrame to see it applied to every numerical column:

# COMMAND ----------

df[["number_of_reviews", "host_listings_count", "bedrooms"]].describe()

# COMMAND ----------

# MAGIC %md <i18n value="b74fc8a9-76d5-446c-aec9-0a4cbb247bef"/>
# MAGIC 
# MAGIC  
# MAGIC Many times, you won't care about the 6th value after the decimal. Let's round our results by calling [**round()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.round.html?highlight=round#pandas.DataFrame.round).

# COMMAND ----------

df[["number_of_reviews", "host_listings_count", "bedrooms"]].describe().round(2)

# COMMAND ----------

# MAGIC %md <i18n value="9d6e0cc1-b817-42b0-b18e-02810cdb8711"/>
# MAGIC 
# MAGIC  
# MAGIC ## Group By
# MAGIC 
# MAGIC Sometimes we will want to see the results of an aggregate function per category in a non-numerical column. 
# MAGIC 
# MAGIC For example, say we wanted to see the average number of bedrooms per neighborhood.
# MAGIC 
# MAGIC In order to do this we first use the [**groupby([columns])**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html) method and specific the category we want to group by. In this case, let's group by **`neighborhood`**.

# COMMAND ----------

df.groupby(["neighborhood"])

# COMMAND ----------

# MAGIC %md <i18n value="236d5b5d-cffb-468d-b63d-87920463e92a"/>
# MAGIC 
# MAGIC  
# MAGIC Then we apply the aggregate function of interest. In this case, **`mean()`** to the column of interest, in this case **`bedrooms`**.
# MAGIC 
# MAGIC **Note:** Here we use **`[["bedrooms"]]`** to select for bedrooms because we could add other columns in addition to bedrooms.

# COMMAND ----------

grouped_df = df.groupby(["neighborhood"])[["bedrooms"]].mean().head(10)
grouped_df

# COMMAND ----------

# MAGIC %md <i18n value="1bf79474-0ec0-4e8b-9c6a-5d9e4fa965f4"/>
# MAGIC 
# MAGIC  
# MAGIC # Reset Index
# MAGIC Typically, row indices are numbers, but they can also be named. In the example above, **`neighborhood`** is now the row index, rather than a column. 
# MAGIC 
# MAGIC We can see that if we print out the columns.

# COMMAND ----------

grouped_df.columns

# COMMAND ----------

# MAGIC %md <i18n value="775492c1-0ecb-45ea-a22d-c26f21e72dcb"/>
# MAGIC 
# MAGIC  
# MAGIC To reset the index to be numbers, and move the current index to be a column, we use [**reset_index()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.reset_index.html).

# COMMAND ----------

reset_df = grouped_df.reset_index()
reset_df

# COMMAND ----------

# MAGIC %md <i18n value="c66e2c3c-b5b4-4414-acb8-6da1066bafc9"/>
# MAGIC 
# MAGIC  
# MAGIC ## Sorting
# MAGIC 
# MAGIC Pandas provides a [**sort_values()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html) method to sort the rows in a **`DataFrame`** or **`Series`**.
# MAGIC 
# MAGIC If called on a **`DataFrame`** you need to specify which column you are sorting by like this **`df.sort_values([col])`**

# COMMAND ----------

sorted_df = df.sort_values(["bedrooms"])
sorted_df[["id","bedrooms"]].head(10)

# COMMAND ----------

# MAGIC %md <i18n value="629120a5-e21a-4fc1-bbc6-26db481023cb"/>
# MAGIC 
# MAGIC  
# MAGIC If applied to a **`Series`** there is only one column, so you don't need to specify:

# COMMAND ----------

df["bedrooms"].sort_values()

# COMMAND ----------

# MAGIC %md <i18n value="609293f8-6e06-4a1a-9e8b-1c9b8bff472b"/>
# MAGIC 
# MAGIC  
# MAGIC By default **`sort_values()`** sorts in ascending order. You can specify the **`ascending=False`** parameter to change it to descending order.

# COMMAND ----------

df["bedrooms"].sort_values(ascending=False)

# COMMAND ----------

# MAGIC %md <i18n value="fffcfb0b-b658-4c92-8acf-c2bbb05aa519"/>
# MAGIC 
# MAGIC  
# MAGIC # NaN 
# MAGIC 
# MAGIC You might have noticed that our **`DataFrame`** contains NaN values. These indicate a missing value. 
# MAGIC 
# MAGIC We have a few ways we can handle missing values. Often having these values present causes problems for computational tasks.
# MAGIC 
# MAGIC First, we can check using the [**isna()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.isna.html#pandas.DataFrame.isna) method, alias for [**isnull()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.isnull.html#pandas.DataFrame.isnull), and the **`sum()`** method to count the number of NaN values present.

# COMMAND ----------

nan_df = df[["security_deposit", "notes"]] # subset of columns with NaNs
nan_df

# COMMAND ----------

nan_df.isna().sum()

# COMMAND ----------

# MAGIC %md <i18n value="dfc8960b-9408-4e30-892e-4e60cea3814a"/>
# MAGIC 
# MAGIC  
# MAGIC ## Dropping NaN
# MAGIC 
# MAGIC One way you can handle NaN is to drop all rows that have NaN values. We can use the [**dropna()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html) method to do that.

# COMMAND ----------

nan_df.dropna()

# COMMAND ----------

# MAGIC %md <i18n value="1582be7b-f6e5-4e24-8898-9dca7a1bcd2f"/>
# MAGIC 
# MAGIC  
# MAGIC ### Impute Columns
# MAGIC 
# MAGIC However, we are throwing away a lot of information when we drop records - in the example above, we removed over 3000 rows.
# MAGIC 
# MAGIC Instead of dropping rows with missing values, we can impute the missing values using [**fillna()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html) and specifying a default value to use.

# COMMAND ----------

nan_df.fillna("Missing")

# COMMAND ----------

# MAGIC %md <i18n value="d21dcf8e-0db0-4477-a80f-a96558f1a6f1"/>
# MAGIC 
# MAGIC  
# MAGIC Oftentimes, we want to impute different values to different columns. For example, with `numeric` values, we can impute with the mean/median/etc. For `categorical` features, imputing with the mode or a special category are common.
# MAGIC 
# MAGIC Let's instead specify that **`security_deposit`** is `$0.00` if it is missing. We can pass in a dictionary to **`fillna()`** that has column names as the key and the value to impute the column with as the value. 
# MAGIC 
# MAGIC You can optionally specify **`inplace=True`** if you want to update the underlying DataFrame.

# COMMAND ----------

nan_df.fillna({"security_deposit": "$0.00", "notes": "Missing"}, inplace=False)

# COMMAND ----------

# MAGIC %md <i18n value="5e800ca5-0801-4a83-84fe-ebe88eae9093"/>
# MAGIC 
# MAGIC  
# MAGIC ## Write to CSV
# MAGIC 
# MAGIC We can write a **`pandas`** DataFrame to a CSV file as shown below using the [**to_csv()**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html) method.

# COMMAND ----------

file_path = DA.paths.working_dir.replace("dbfs:", "/dbfs") + ".csv"
df.to_csv(file_path, index=False)

# COMMAND ----------

# MAGIC %md <i18n value="c1bbcbf6-9c19-450a-ac92-1486f30a3f3c"/>
# MAGIC 
# MAGIC  
# MAGIC We can then read our csv file back in with **`read_csv`**.

# COMMAND ----------

load_df = pd.read_csv(file_path)
load_df.head()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
