# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using External Libraries with UDFs
# MAGIC 
# MAGIC This lesson introduces more advanced text processing steps by applying single-node libraries in parallel for feature preprocessing.
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Learn about single node NLP libraries
# MAGIC * Create user defined functions to parallelize library calls
# MAGIC * Lemmatize and stem your tokens

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single-Machine Natural Language Processing Libraries
# MAGIC 
# MAGIC Just because a library is built for a single node doesn't mean you can't apply it to your code in parallel! In general, anything that is a rule-based transformation can be applied in parallel.
# MAGIC 
# MAGIC Below is a list of popular single node libraries that contain rule-based transformation functionalities:
# MAGIC * <a href="https://www.nltk.org/" target="_blank">NLTK</a>
# MAGIC * <a href="https://spacy.io/" target="_blank">spaCy</a>
# MAGIC * <a href="https://radimrehurek.com/gensim/" target="_blank">Gensim</a>
# MAGIC * <a href="https://textblob.readthedocs.io/en/dev/#" target="_blank">TextBlob</a>
# MAGIC * <a href="https://allennlp.org/" target="_blank">AllenNLP</a>
# MAGIC 
# MAGIC We will be using `NLTK`, `vaderSentiment`, and `TextBlob` in this lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## User Defined Function (UDF)
# MAGIC 
# MAGIC While the distributed libraries we looked at in the last lesson are optimized for speed + distributed computation, sometimes they don't have all the functionalities that we need. In those cases, we will have to rely on more developed single-node libraries. We can write a **user defined function** to apply these libraries to our DataFrame in parallel.
# MAGIC 
# MAGIC UDFs provide flexibility for when built-in functions are lacking, but UDFs have several drawbacks: 
# MAGIC * UDFs cannot be optimized by the Catalyst Optimizer
# MAGIC * The function **has to be serialized** and sent out to the executors
# MAGIC * In the case of Python, there is even more overhead - we have to **spin up a Python interpreter** on every Executor to run the UDF (e.g. Python UDFs much slower than Scala UDFs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stemming and Lemmatizing with NLTK
# MAGIC 
# MAGIC We discussed earlier how to make our strings more comparable by tokenizing and lowercasing. But so far, strings like "run", "running", and "ran" are still treated as completely unrelated words if we were to compare them; however, their only difference is purely a result of grammatical structure. To resolve this, we are going to use a process called **stemming** which is going to 'chop' off the ends of words to get it to its base form. Since stemming only removes letters from our string, often times it results in strings that aren't real words.
# MAGIC 
# MAGIC Another more involved process that attempts to change strings into more comparable forms is called **lemmatizing**. It tries to find the dictionary form - also called the lemma - of a word. This means the results of lemmatizing are real words that we recognize.

# COMMAND ----------

# Stemming vs Lemmatizing
import nltk
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer

nltk.download("wordnet")
stemmer = PorterStemmer()
lemmatizer = WordNetLemmatizer()

words = ["running", "ponies", "pony", "dogs", "people", "geese"]
word_forms = [(word, stemmer.stem(word), lemmatizer.lemmatize(word)) for word in words]

for orig, stemmed, lemmatized in word_forms:
    print(f"Original: {orig}  \tStemmed: {stemmed} \tLemmatized:{lemmatized}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now that we understand the concept of stemming and lemmatizing and how to call a function as a UDF, we're going combine these skills to stem and lemmatize our reviews dataset's `CleanTokens` using the NLTK functions `PorterStemmer` and `WordNetLemmatizer`.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stemming UDF

# COMMAND ----------

# MAGIC %md
# MAGIC In Spark 2.x, this is an example of how we can write UDFs.

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, StringType

processedDF = spark.read.parquet("/mnt/training/reviews/tfidf.parquet")

@udf(ArrayType(StringType()))
def stem_udf(tokens):
  ps = PorterStemmer()
  return [ps.stem(token) for token in tokens]

# add StemTokens column
stemmedDF = processedDF.withColumn("StemTokens", stem_udf(col("CleanTokens")))
display(stemmedDF.select("StemTokens", "CleanTokens").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vectorized UDF
# MAGIC 
# MAGIC As of Spark 2.3, there are Vectorized UDFs available in Python to help speed up the computation.
# MAGIC 
# MAGIC * [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow)
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC 
# MAGIC Vectorized UDFs utilize Apache Arrow to speed up computation. Let's see how that helps improve our processing time.
# MAGIC 
# MAGIC The user-defined functions are executed by:
# MAGIC * [Apache Arrow](https://arrow.apache.org/), is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost. See more [here](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).
# MAGIC * pandas inside the function, to work with pandas instances and APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC On the left, this is how a function is applied to a series, e.g. `Series.apply(..., axis='index')`. On the right, `pandas_udf` is applying the function to a batch of `value`s. For instance, in our case, each `value` here is an array of strings or tokens.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/301/pandas_udf_2.png" height="600" width ="400">
# MAGIC 
# MAGIC `pandas_udf` works similarly with pandas's `Dataframe.apply(..., axis='index')`
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/301/pandas_udf_1.png" height="600" width ="400">

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

### In order to correctly use pandas_udf, the return type should be specified
### An alternative way to specify type is: @pandas_udf(ArrayType(StringType()))
@pandas_udf("ARRAY<STRING>")
def stem_udf(tokens_batch: pd.Series) -> pd.Series:
  ps = PorterStemmer()
  return pd.Series([list(map(ps.stem, tokens)) for tokens in tokens_batch])

stemmedDF = processedDF.withColumn("StemTokens", stem_udf(col("CleanTokens")))
display(stemmedDF.select("StemTokens", "CleanTokens").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC There is a new type of Pandas UDF in Spark 3.0+, called scalar iterator UDF.
# MAGIC 
# MAGIC From this [Databricks blog post](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html) by Hyukjin Kwon:
# MAGIC 
# MAGIC > It is a variant of Series to Series, and the type hints can be expressed as Iterator[pd.Series] -> Iterator[pd.Series]. The function takes and outputs an iterator of pandas.Series.
# MAGIC 
# MAGIC > The length of the whole output must be the same length of the whole input. Therefore, it can prefetch the data from the input iterator as long as the lengths of entire input and output are the same. The given function should take a single column as input.
# MAGIC 
# MAGIC > It is useful when the UDF execution requires expensive initialization of some state. 
# MAGIC 
# MAGIC Notice that in the code below, `PorterStemmer()` is only initialized and loaded once, rather than once for every batch.

# COMMAND ----------

from typing import Iterator

@pandas_udf("ARRAY<STRING>")
def stem_scalar_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
  ps = PorterStemmer()
  for tokens_batch in iterator:
    yield pd.Series([list(map(ps.stem, tokens)) for tokens in tokens_batch])
    
stemmedDF = processedDF.withColumn("StemTokens", stem_scalar_udf(col("CleanTokens")))
display(stemmedDF.select("StemTokens", "CleanTokens").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lab: Lemmatizing UDF
# MAGIC 
# MAGIC Now that you have seen the examples of stemming words using a UDF, a pandas UDF and also a scalar iterator UDF. It's your turn to write a UDF of any type that lemmatizes words! 

# COMMAND ----------

# ANSWER 
### Method 1 without using pandas_udf

# create UDF
@udf(ArrayType(StringType()))
def lemma_udf(tokens):
  nltk.download("wordnet")
  lemmatizer = WordNetLemmatizer()
  return [lemmatizer.lemmatize(token) for token in tokens]

# add LemmaTokens column
lemmaDF = processedDF.withColumn("LemmaTokens", lemma_udf(col("CleanTokens")))
display(lemmaDF.select("Tokens", "LemmaTokens").limit(2))

# COMMAND ----------

# ANSWER
### Method 2 using pandas_udf

@pandas_udf(ArrayType(StringType()))
def lemma_udf(tokens_batch: pd.Series) -> pd.Series:
  nltk.download("wordnet")
  lemmatizer = WordNetLemmatizer()
  return pd.Series([list(map(lemmatizer.lemmatize, tokens)) for tokens in tokens_batch])

# add LemmaTokens column
lemmaDF = processedDF.withColumn("LemmaTokens", lemma_udf(col("CleanTokens")))
display(lemmaDF.select("LemmaTokens", "Tokens").limit(2))

# COMMAND ----------

# ANSWER
### Method 3 using scalar iterator udf 

@pandas_udf(ArrayType(StringType()))
def lemma_scalar_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
  nltk.download("wordnet")
  lemmatizer = WordNetLemmatizer()
  for tokens_batch in iterator:
    yield pd.Series([list(map(lemmatizer.lemmatize, tokens)) for tokens in tokens_batch])
    
### add LemmaTokens column
lemmaDF = processedDF.withColumn("LemmaTokens", lemma_scalar_udf(col("CleanTokens")))
display(lemmaDF.select("LemmaTokens", "Tokens").limit(2))    

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
