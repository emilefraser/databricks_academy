# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="92f0d116-aa27-4e1f-9be2-9eb102794cf6"/>
# MAGIC 
# MAGIC 
# MAGIC # Data Visualization
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
# MAGIC Explore data visualization of **`pandas`** DataFrames using:
# MAGIC - Databricks built-in plotting
# MAGIC - **`pandas`** plotting methods
# MAGIC - **`seaborn`** plotting functionality
# MAGIC 
# MAGIC Let's import **`pandas`** and our Airbnb Dataset

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd

# COMMAND ----------

file_path = f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv".replace("dbfs:", "/dbfs")
df = pd.read_csv(file_path)
df.head(3)

# COMMAND ----------

# MAGIC %md <i18n value="46852d14-6b5c-4906-8a87-2d87f099bfeb"/>
# MAGIC 
# MAGIC  
# MAGIC ## Built-in Plotting
# MAGIC 
# MAGIC Databricks provides built in data visualization tools we can use in a Databricks notebook. 
# MAGIC 
# MAGIC In order to use them, we use the built-in **`display()`** function Databricks provides on a pandas DataFrame.
# MAGIC 
# MAGIC By default, we just see the `DataFrame` as a table. However, if you click the bottom middle bar chart icon we can switch to plotting mode.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md <i18n value="c609e59a-b0ce-40fc-bd71-24e10730b6b6"/>
# MAGIC 
# MAGIC  
# MAGIC ## Plot Options
# MAGIC 
# MAGIC In order to change the kind of plot we see we can click "Plot Options" next to the bar chart icon.
# MAGIC 
# MAGIC From there we can specify what kind of plot we want on which columns of the DataFrame.
# MAGIC 
# MAGIC For example, let's say we wanted to view the average number of bedrooms per neighborhood. 
# MAGIC 
# MAGIC To do this:
# MAGIC 
# MAGIC 1. Click Plot Options.
# MAGIC 2. Add **`neighbourhood`** to the keys.
# MAGIC 3. Add **`bedrooms`** to the values.
# MAGIC 3. Change the aggregate function to average.
# MAGIC 4. Make sure we have a bar chart selected.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md <i18n value="6d9956db-6e60-4533-966e-1be99415c1b8"/>
# MAGIC 
# MAGIC  
# MAGIC Note that initially it only will show a preview of the first 1000 rows, but when we click **`apply`** it works on all of them.

# COMMAND ----------

# MAGIC %md <i18n value="3c4eb78d-05a2-4133-905b-90411fffdb02"/>
# MAGIC 
# MAGIC  
# MAGIC ## Pandas Plotting
# MAGIC 
# MAGIC **`pandas`** also provides some plotting functionality. 
# MAGIC 
# MAGIC We can create a histogram using the **`hist()`** method on a **`Series`**.
# MAGIC 
# MAGIC Let's create a histogram of the number of bedrooms:

# COMMAND ----------

df["bedrooms"].hist()

# COMMAND ----------

# MAGIC %md <i18n value="8eec6709-d9aa-499c-ab29-37d51ee8dfc1"/>
# MAGIC 
# MAGIC 
# MAGIC We can also specify the number of bins by passing an argument to **`bins`** parameter.

# COMMAND ----------

df["bedrooms"].hist(bins=20)

# COMMAND ----------

# MAGIC %md <i18n value="14bed99e-90e8-4a85-bfd5-99be4ff0563e"/>
# MAGIC 
# MAGIC  
# MAGIC We can also box plots with pandas.
# MAGIC 
# MAGIC We use the method **`boxplot([cols])`** on a **`DataFrame`** to create a box plot for each of the specified columns:

# COMMAND ----------

df.boxplot(["bedrooms", "bathrooms"])

# COMMAND ----------

# MAGIC %md <i18n value="9c6d8ce4-ef18-4c32-b9e1-fe6ada690af5"/>
# MAGIC 
# MAGIC  
# MAGIC # Seaborn
# MAGIC 
# MAGIC [seaborn](https://seaborn.pydata.org/) is a very popular data visualization library that works with pandas DataFrames. 
# MAGIC 
# MAGIC It is popular for both being relatively easy to use and for producing nice looking visualizations.
# MAGIC 
# MAGIC Let's import **`seaborn`**: it is common practice to use **`sns`** as the alias.

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

# MAGIC %md <i18n value="cec27556-7c8f-4991-89ad-d060836aaf98"/>
# MAGIC 
# MAGIC  
# MAGIC ## Scatter plot
# MAGIC 
# MAGIC Let's first create a scatter plot. We'll plot **`bedrooms`** cases on the x-axis and **`bathrooms`** on the y-axis. 
# MAGIC 
# MAGIC In order to do this, we call **`sns.scatterplot(data=, x=, y=)`**
# MAGIC 
# MAGIC We provide a **`DataFrame`** as the data parameter, and the column names we want for the x and y parameters.

# COMMAND ----------

sns.scatterplot(data=df, x="bedrooms", y="bathrooms")

# COMMAND ----------

# MAGIC %md <i18n value="f8901310-8093-4568-944e-ac277d0b3da0"/>
# MAGIC 
# MAGIC  
# MAGIC You might also want to plot a line of best fit for the scatter plot. 
# MAGIC 
# MAGIC We can do this by using the same parameters but for the **`regplot()`** function:

# COMMAND ----------

sns.regplot(data=df, x="bedrooms", y="bathrooms")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
