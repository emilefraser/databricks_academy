# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # NLP Course Overview
# MAGIC ## Foundations of Scalable Natural Language Processing
# MAGIC 
# MAGIC Learn the fundamentals of natural language processing (NLP) and how to scale it as you solve classification, sentiment analysis, and text wrangling tasks. We will cover how to perform distributed model inference and hyperparameter search, as well as build distributed NLP models. Working through code examples and assignments in Python, students will learn how to use dimensionality reduction techniques, apply pre-trained word embeddings, leverage MLflow for experiment tracking and model management, and apply NLP models to massive datasets with Pandas UDFs.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lessons
# MAGIC 0. NLP Course Overview
# MAGIC 0. How to pre-process text at scale
# MAGIC 0. UDFs with External Libraries
# MAGIC 0. Embeddings
# MAGIC 0. Sentiment Analysis with LSTM and MLflow
# MAGIC 0. Hyperparameter Tuning using Hyperopt
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Audience
# MAGIC * Primary Audience: Data scientists and data analysts
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Prerequisites
# MAGIC * Experience with PySpark DataFrames
# MAGIC * Web browser: **Chrome**
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Cluster Requirements
# MAGIC * See your instructor for specific requirements

# COMMAND ----------

# MAGIC %pip install wordcloud==1.7

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson (see the next cell).

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC # What is NLP?
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
# MAGIC * Learn the motivation behind NLP
# MAGIC * Definition of NLP
# MAGIC * Use cases
# MAGIC * Get started exploring the Amazon Food Reviews Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definition
# MAGIC 
# MAGIC Natural language processing, often abbreviated NLP, is a subfield of artificial intelligence concerned with the interactions between computers and human (natural) languages; in particular how to program computers to process and analyze large amounts of natural language data.
# MAGIC 
# MAGIC 
# MAGIC ### Rule-Based vs Statistical NLP
# MAGIC 
# MAGIC A rule-based NLP approach, the older more traditional approach, involves hand-coding a set of rules and as a result, is not as robust to variation in language. The statistical approach is based on probabilistic results and is able to "learn" without being explicitly programmed.
# MAGIC 
# MAGIC In this course, we will apply concepts from both of these NLP approaches.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why NLP?
# MAGIC 
# MAGIC Natural Language Processing (NLP) is a broad field, encompassing a wide range of use cases, such as:
# MAGIC 
# MAGIC * Sentiment Analysis: Analyzing sentiment of customer product feedback
# MAGIC * Question Answering: Answering questions based on an article
# MAGIC * Machine Translation: Translating from one language to another
# MAGIC * Chat bots: Chatbots for answering customer questions or requests
# MAGIC * Automatic Summarization: Summarization of news articles or various tweets about a common subject

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Reviews Dataset Use Case
# MAGIC 
# MAGIC Input data for NLP is often in an unstructured format such as text or recorded speech, with the majority of publicly available NLP datasets coming from social media and other websites.
# MAGIC 
# MAGIC Throughout the rest of this course we will be applying our NLP concepts to the following Amazon Food Reviews dataset which can also be viewed at this Kaggle
# MAGIC <a href="https://www.kaggle.com/snap/amazon-fine-food-reviews" target="_blank">link</a>.
# MAGIC 
# MAGIC Source of the data: J. McAuley and J. Leskovec. [From amateurs to connoisseurs: modeling the evolution of user expertise through online reviews](http://i.stanford.edu/~julian/pdfs/www13.pdf). WWW, 2013.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the DataFrame
# MAGIC 
# MAGIC In the next cell, we will read in the `reviews.csv` and display it.

# COMMAND ----------

text_df = spark.read.csv("/mnt/training/reviews/reviews.csv", header=True, escape='"')
display(text_df.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting Columns
# MAGIC 
# MAGIC We are most interested in processing the `Text` column of the DataFrame to create features for predicting the `Score` for a given product. We are going to select these columns: `Id`, `ProductId`, `Score`, `Summary`, and `Text`, then perform exploratory data analysis (EDA) and data cleansing.
# MAGIC 
# MAGIC We will also cache the dataset to make it faster to perform operations.

# COMMAND ----------

text_df = text_df.select("Id", "ProductId", "Score", "Summary", "Text")

text_df.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Distribution of Scores in Dataset
# MAGIC 
# MAGIC To view the data as a Bar Chart, select the `Plot Options` and change the visualization to `Bar Chart`. Ensure that the `Score` is your key, and the `count` is your value in the Bar chart.

# COMMAND ----------

from pyspark.sql.functions import col

display(text_df.groupBy("Score").count().sort(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Skew
# MAGIC Wow! We have quite a bit of data skew here! Most of the entries are 5 and 4 star reviews, and 1 star reviews are more prevalent than 2 or 3 star reviews (is that in alignment with how you rate things?). We will discuss various ways to deal with this later.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wordcloud
# MAGIC 
# MAGIC Now we're going to take a look at the data inside the text column. A good way to visualize a corpus of words is by creating a [wordcloud](https://en.m.wikipedia.org/wiki/Tag_cloud)! A wordcloud will create an image with more frequently appearing words larger and closer to the center than words that appear less frequently in the text.
# MAGIC 
# MAGIC Below are 3 different wordclouds for 3 different `Text` entries of the DataFrame.

# COMMAND ----------

from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

def wordcloud_draw(text, title="", color="white"):
    """
    Plots wordcloud of string text after removing stopwords
    """
    cleaned_word = " ".join([word for word in text.split() if "http" not in word])
    wordcloud = WordCloud(
        stopwords=STOPWORDS, background_color=color, width=1000, height=1000
    ).generate(cleaned_word)
    plt.figure(1, figsize=(8, 8))
    plt.imshow(wordcloud)
    plt.title(title)
    plt.axis("off")
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Wordcloud of Text in Row 1: `Good Quality Dog Food`

# COMMAND ----------

list_texts = text_df.select("Text", "Summary").limit(3).collect()
row = 0  # First row
wordcloud_draw(
    list_texts[row][0], list_texts[row][1], "white"
)  # Background color is white

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Wordcloud of Text in Row 2: `Not as Advertised`

# COMMAND ----------

row = 1
wordcloud_draw(list_texts[row][0], list_texts[row][1], "gray")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Wordcloud of Text in Row 3: `"Delight" says it all`

# COMMAND ----------

row = 2
wordcloud_draw(list_texts[row][0], list_texts[row][1], "black")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
