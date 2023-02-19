# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="77c31689-d17b-4d25-b97b-53e046c50b10"/>
# MAGIC 
# MAGIC 
# MAGIC # Scaling Pandas with Spark
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC - Demonstrate the similarities of the pandas API on Spark API with the pandas API
# MAGIC - Understand the differences in syntax for the same DataFrame operations in pandas API on Spark vs PySpark

# COMMAND ----------

# MAGIC %md <i18n value="8fad101f-86e7-4d33-9792-83cd3c87c23e"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Recall from the cloud computing lesson the open source technology [Apache Spark](https://spark.apache.org/), which is an open-source data processing engine that manages distributed processing of large data sets. 
# MAGIC 
# MAGIC In this course, we have been using Pandas, which is a great library for data analysis, but it only runs on one machine, it is not distributed. Luckily, we can use Spark just like traditional Pandas with the Pandas API on Spark. 
# MAGIC 
# MAGIC The pandas API on Spark project makes data scientists more productive when interacting with big data, by implementing the pandas DataFrame API on top of Apache Spark. By unifying the two ecosystems with a familiar API, pandas API on Spark offers a seamless transition between small and large data. 
# MAGIC 
# MAGIC See this <a href="https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html" target="_blank">blog post</a> for more information on using Pandas syntax on Spark.

# COMMAND ----------

# MAGIC %md <i18n value="9112c122-6e72-4fe2-85f3-c1786b8d0620"/>
# MAGIC 
# MAGIC  
# MAGIC 
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/301/31gb.png" width="900"/>
# MAGIC </div>
# MAGIC 
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/301/95gb.png" width="900"/>
# MAGIC </div>
# MAGIC 
# MAGIC **Pandas** DataFrames are mutable, eagerily evaluated, and maintain row order. They are restricted to a single machine, and are very performant when the data sets are small, as shown in a).
# MAGIC 
# MAGIC **Spark** DataFrames are distributed, lazily evaluated, immutable, and do not maintain row order. They are very performant when working at scale, as shown in b) and c).
# MAGIC 
# MAGIC **pandas API on Spark** provides the best of both worlds: pandas API with the performance benefits of Spark. However, it is not as fast as implementing your solution natively in Spark, and let's see why below.

# COMMAND ----------

# MAGIC %md <i18n value="812c6711-72b9-464c-86f8-66b93dd7e0d2"/>
# MAGIC 
# MAGIC  
# MAGIC ## InternalFrame
# MAGIC 
# MAGIC The InternalFrame holds the current Spark DataFrame and internal immutable metadata.
# MAGIC 
# MAGIC It manages mappings from pandas API on Spark column names to Spark column names, as well as from pandas API on Spark index names to Spark column names. 
# MAGIC 
# MAGIC If a user calls some API, the pandas API on Spark DataFrame updates the Spark DataFrame and metadata in InternalFrame. It creates or copies the current InternalFrame with the new states, and returns a new pandas API on Spark DataFrame.
# MAGIC 
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/301/InternalFramePs.png" width="900"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="118a32b5-df6e-4679-884c-8077c2d6aa09"/>
# MAGIC 
# MAGIC  
# MAGIC ### Read in the dataset
# MAGIC 
# MAGIC * PySpark
# MAGIC * pandas
# MAGIC * pandas API on Spark

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md <i18n value="a5801cbc-ab2a-444a-ad8a-a96791cf0596"/>
# MAGIC 
# MAGIC  
# MAGIC Just like how we can read in data from a CSV file in pandas, we can read in data from a file in spark.

# COMMAND ----------

spark_df = spark.read.csv(f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv", header="true", inferSchema="true", multiLine="true", escape='"')
display(spark_df)

# COMMAND ----------

# MAGIC %md <i18n value="8f40703f-c097-4532-9bae-d56b07dd6907"/>
# MAGIC 
# MAGIC 
# MAGIC Read in CSV with pandas

# COMMAND ----------

import pandas as pd

pandas_df = pd.read_csv(f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv".replace("dbfs:", "/dbfs"))
pandas_df.head()

# COMMAND ----------

# MAGIC %md <i18n value="3092efbf-6f55-45ca-9ec7-4a45250d55f5"/>
# MAGIC 
# MAGIC 
# MAGIC Read in CSV with pandas API on Spark. You'll notice pandas API on Spark generates an index column for you, like in pandas.

# COMMAND ----------

import pyspark.pandas as ps

df = ps.read_csv(f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv", inferSchema="true", multiLine="true", escape='"')
df.head()

# COMMAND ----------

# MAGIC %md <i18n value="ec4b222c-7a5f-49d7-a913-bc5a1c6c269f"/>
# MAGIC 
# MAGIC 
# MAGIC ### <a href="https://koalas.readthedocs.io/en/latest/user_guide/options.html#default-index-type" target="_blank">Index Types</a>
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/301/koalas_index.png)

# COMMAND ----------

ps.set_option("compute.default_index_type", "distributed-sequence")
df_dist_sequence = ps.read_csv(f"{DA.paths.datasets}/sf-airbnb/sf-airbnb.csv", inferSchema="true", multiLine="true", escape='"')
df_dist_sequence.head()

# COMMAND ----------

# MAGIC %md <i18n value="2fd38d18-35ae-4971-825f-a6fd5cbc7b64"/>
# MAGIC 
# MAGIC 
# MAGIC ### Converting to pandas API on Spark DataFrame to/from Spark DataFrame

# COMMAND ----------

# MAGIC %md <i18n value="43331470-511a-4975-b075-3cf006da4e23"/>
# MAGIC 
# MAGIC 
# MAGIC Creating a pandas API on Spark DataFrame from PySpark DataFrame

# COMMAND ----------

df = ps.DataFrame(spark_df)
display(df)

# COMMAND ----------

# MAGIC %md <i18n value="9579fb05-e9a2-4774-a4ad-d1988e0249ea"/>
# MAGIC 
# MAGIC 
# MAGIC Alternative way of creating a pandas API on Spark DataFrame from PySpark DataFrame

# COMMAND ----------

df = spark_df.to_pandas_on_spark()
display(df)

# COMMAND ----------

# MAGIC %md <i18n value="22d68896-91dc-432b-afd8-94bdfb962c09"/>
# MAGIC 
# MAGIC 
# MAGIC Go from a pandas API on Spark DataFrame to a Spark DataFrame

# COMMAND ----------

display(df.to_spark())

# COMMAND ----------

# MAGIC %md <i18n value="523332ab-1890-411e-a830-f81476dc0aba"/>
# MAGIC 
# MAGIC 
# MAGIC ### Value Counts

# COMMAND ----------

# MAGIC %md <i18n value="d76a4309-2ad4-4ba9-9948-96d3f1dab224"/>
# MAGIC 
# MAGIC 
# MAGIC Get value counts of the different property types with PySpark

# COMMAND ----------

display(spark_df.groupby("property_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md <i18n value="63d5ecb1-8d6f-47fd-882f-8dd9d13d9748"/>
# MAGIC 
# MAGIC 
# MAGIC Get value counts of the different property types with pandas API on Spark

# COMMAND ----------

df["property_type"].value_counts()

# COMMAND ----------

# MAGIC %md <i18n value="cb1c7961-4a96-4c18-9e0b-b9bbd35d5096"/>
# MAGIC 
# MAGIC 
# MAGIC ### Visualizations
# MAGIC 
# MAGIC Based on the type of visualization, the pandas API on Spark has optimized ways to execute the plotting.
# MAGIC <br><br>
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/301/ps_plotting.png)

# COMMAND ----------

df["bedrooms"].hist(bins=20)

# COMMAND ----------

# MAGIC %md <i18n value="f449a17a-d308-4729-9696-4f6d804168f6"/>
# MAGIC 
# MAGIC 
# MAGIC ### SQL on pandas API on Spark DataFrames

# COMMAND ----------

ps.sql("SELECT distinct(property_type) FROM {df}", df=df)

# COMMAND ----------

# MAGIC %md <i18n value="b9935020-093d-44f1-b84d-95b676d0d7cc"/>
# MAGIC 
# MAGIC 
# MAGIC ### Interesting Facts
# MAGIC 
# MAGIC * With pandas API on Spark you can read from Delta Tables and read in a directory of files
# MAGIC * If you use apply on a pandas API on Spark DF and that DF is <1000 (by default), pandas API on Spark will use pandas as a shortcut - this can be adjusted using **`compute.shortcut_limit`**
# MAGIC * When you create bar plots, the top n rows are only used - this can be adjusted using **`plotting.max_rows`**
# MAGIC * How to utilize **`.apply`** <a href="https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.DataFrame.apply.html#databricks.koalas.DataFrame.apply" target="_blank">docs</a> with its use of return type hints similar to pandas UDFs
# MAGIC * How to check the execution plan, as well as caching a pandas API on Spark DF (which aren't immediately intuitive)
# MAGIC * Koalas are marsupials whose max speed is 30 kph (20 mph)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
