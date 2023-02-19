# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="3a4f1c43-9316-4e40-91a0-2e61ae23c8a4"/>
# MAGIC 
# MAGIC 
# MAGIC # Pandas
# MAGIC 
# MAGIC **<a href="https://pandas.pydata.org/pandas-docs/stable/reference/index.html" target="_blank">Pandas</a>** is a popular Python library among data scientists with high performing, easy-to-use data structures and data analysis tools.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Explain what **`pandas`** is and why it's so popular
# MAGIC * Create and manipulate **`pandas`** **`DataFrame`** and **`Series`**
# MAGIC * Perform operations on **`pandas`** objects
# MAGIC 
# MAGIC First, let us import **`pandas`** with the alias **`pd`** so we can refer to the library without having to type **`pandas`** out each time. **`pandas`** is pre-installed on Databricks.

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md <i18n value="23d65c0c-d3f7-4e13-a0ee-f82272eb4daa"/>
# MAGIC 
# MAGIC 
# MAGIC #### Why `pandas`?
# MAGIC 
# MAGIC * More and more, data is leading decision making.
# MAGIC * Excel is great but what if...
# MAGIC   - You want to automate your analysis so it re-runs on new data each day?
# MAGIC   - You want to build a code base to share with your colleagues?
# MAGIC   - You want more robust analyses to feed a business decision?
# MAGIC   - You want to do machine learning?
# MAGIC * **`pandas`** is of the core libraries used by data analysts and data scientists in Python.

# COMMAND ----------

# MAGIC %md <i18n value="68744e01-4fc2-4282-9fae-bf6898d0c264"/>
# MAGIC 
# MAGIC 
# MAGIC ## `DataFrame`
# MAGIC 
# MAGIC We have seen how different data types provide different kinds of data and functionality. 
# MAGIC 
# MAGIC **`pandas` is a library that provides data types and functions that allows us to do rigorous, programmatic data analysis.** 
# MAGIC - The core **`pandas`** data type is called a **`DataFrame`**.
# MAGIC 
# MAGIC A [**DataFrame**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) is a two dimensional table of named rows and columns, similar to a SQL table. 
# MAGIC 
# MAGIC - The **`DataFrame`** class has a **`data`** attribute for the data in the table that we have to define when we instantiate a **`DataFrame`** object.
# MAGIC 
# MAGIC Let's say we want to make the following table into a **`DataFrame`**:
# MAGIC 
# MAGIC | Name    | Age | Job|
# MAGIC | ----------- | ----------- | ----------- | 
# MAGIC | John   | 30    | Journalist |
# MAGIC | Mary    | 30       | Programmer |
# MAGIC | Abe     | 40       | Chef |
# MAGIC 
# MAGIC One way to do this is to create a list of lists where each list in the list represents a row of data:

# COMMAND ----------

data = [["John", 30, "Journalist"], ["Mary", 30, "Programmer"], ["Abe", 40, "Chef"]]

df = pd.DataFrame(data=data)
df

# COMMAND ----------

# MAGIC %md <i18n value="96e03931-0ce6-4d71-914f-93f192c90931"/>
# MAGIC 
# MAGIC 
# MAGIC Recall that we create an object of a custom class like **`object = Class()`**. Since **`DataFrame`** is defined in **`pandas`** we use **`pd.DataFrame()`**.

# COMMAND ----------

# MAGIC %md <i18n value="75f28ee2-cedb-477e-ad86-bf4684cff899"/>
# MAGIC 
# MAGIC 
# MAGIC ### Adding Column Names
# MAGIC 
# MAGIC The 0, 1, 2 column names above are default values. To specify the column names we want, **`DataFrame`** has another attribute: **`columns`**

# COMMAND ----------

cols = ["Name", "Age", "Job"]
df = pd.DataFrame(data=data, columns=cols)
df

# COMMAND ----------

# MAGIC %md <i18n value="12dcbd4f-2636-41b1-a103-88b1a7325494"/>
# MAGIC 
# MAGIC 
# MAGIC ## `Series`
# MAGIC 
# MAGIC The other main data type that **`pandas`** provides is the [**Series**](https://pandas.pydata.org/docs/reference/api/pandas.Series.html).
# MAGIC 
# MAGIC A **`Series`** is just one column of the **`DataFrame`**.
# MAGIC 
# MAGIC We can select a **`Series`** in two ways:
# MAGIC 
# MAGIC 1. **`df["column_name"]`**
# MAGIC 2. **`df.column_name`**
# MAGIC 
# MAGIC Let's select the Age column below:

# COMMAND ----------

df["Age"]

# COMMAND ----------

df.Age

# COMMAND ----------

# MAGIC %md <i18n value="46985f3d-66e6-4bb3-b305-10e2db222867"/>
# MAGIC 
# MAGIC 
# MAGIC It is preferred to use **`df["column_name"]`** to access a column. The **`df.column_name`** notation does not work well when there are spaces in the column names.

# COMMAND ----------

# MAGIC %md <i18n value="a0b445c2-3e9a-4814-b4c9-249a7d2dabeb"/>
# MAGIC 
# MAGIC 
# MAGIC ## dtypes
# MAGIC 
# MAGIC If you look at the **`Series`** object above you can see **`dtype: int64`**. 
# MAGIC 
# MAGIC In **`pandas`** *dtypes*, which is an abbreviation for data types, refers to the type of the values in the column. 
# MAGIC 
# MAGIC We will next look at some of the methods and functionality that **`DataFrame`** and **`Series`** provide, but much like how object types determine what you can do with them, the [**dtype**](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dtypes.html) of a column determines what functionality we can do with it.
# MAGIC 
# MAGIC For example, we can take the average of a numeric column, but not a non-numeric one. 
# MAGIC 
# MAGIC The **`dtypes`** of columns are specific to **`pandas`**, but the most common ones are very similar to the built-in Python types:
# MAGIC 
# MAGIC 1. **`object`** is just text, which is similar to strings
# MAGIC 2. **`int64`** are integers
# MAGIC 3. **`float64`** are float
# MAGIC 4. **`bool`** are boolean
# MAGIC 
# MAGIC 
# MAGIC We can view the **`dtypes`** of every column in our **`DataFrame`** by accessing its **`dtypes`** attribute:

# COMMAND ----------

# MAGIC %md <i18n value="b129d939-a308-4682-b818-33498bf83f87"/>
# MAGIC 
# MAGIC 
# MAGIC ## `Series` Operations
# MAGIC 
# MAGIC We can use operations on **`Series`** of a certain dtype that are similar to the operations we can do with the dtype's similar built-in counterpart. 
# MAGIC 
# MAGIC These operations act element-wise. 
# MAGIC 
# MAGIC For example, we can add **`int64`** **`Series`** similarly to how we can add integer values in Python.

# COMMAND ----------

df["Age"] + df["Age"]

# COMMAND ----------

# MAGIC %md <i18n value="eab93237-f960-44fe-9840-6d82497b1e29"/>
# MAGIC 
# MAGIC 
# MAGIC We can use all basic integer operations here:

# COMMAND ----------

df["Age"] * 3 - 1 

# COMMAND ----------

# MAGIC %md <i18n value="e6713871-d93a-4b68-ae8f-a08d14e871c0"/>
# MAGIC 
# MAGIC 
# MAGIC #### Selecting a value from a **`Series`**
# MAGIC 
# MAGIC Sometimes we will want to pull out a value in a **`Series`**. We can index into a **`Series`** similar to how we index into a list to pull out values:

# COMMAND ----------

df["Age"][0]

# COMMAND ----------

# MAGIC %md <i18n value="1ce3e63c-3e11-4b0a-8078-b6584146c9e5"/>
# MAGIC 
# MAGIC 
# MAGIC ## Selecting a Subset of Columns
# MAGIC 
# MAGIC We have seen how to select a given column as a **`Series`**.
# MAGIC 
# MAGIC We can also select a subset of columns as a **`DataFrame`**.
# MAGIC 
# MAGIC We can select a subset of columns like this:
# MAGIC 
# MAGIC **`df[[col_1, col_2, col_3, ...]]`**
# MAGIC 
# MAGIC Let's select only the Name and Age columns:

# COMMAND ----------

df[["Name", "Age"]]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
