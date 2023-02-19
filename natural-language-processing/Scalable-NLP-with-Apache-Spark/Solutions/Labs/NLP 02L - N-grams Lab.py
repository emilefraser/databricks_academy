# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # N-grams Lab
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC * Learn what n-grams are
# MAGIC * Generate n-grams for each review

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Another commonly used preprocessing step is the generation of n-grams, an ordered sequence of `n` tokens. This can be important when there are meaningful phrases that are made up of multiple words in a specific order. For example knowing that "really good" occurred is more important than just knowing that the tokens "really" and "good" appeared in the text. Sometimes using `n > 2` may be helpful such as extracting the phrase "really highly recommend."
# MAGIC 
# MAGIC Here is an example of the n-grams with n = 1, 2, and 3 for a simple sentence:
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/trigram-updated.png)
# MAGIC 
# MAGIC In addition to using n-grams on a word level, often times they are also used on a character level so that subwords can be learned and typos can be dealt with.
# MAGIC 
# MAGIC **Note:** n-grams with `n = 1` is the same as the original tokens list.

# COMMAND ----------

# MAGIC %md
# MAGIC Load in our tokenized and processed DataFrame.

# COMMAND ----------

processed_df = spark.read.parquet("/mnt/training/reviews/tfidf.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC One implementation of n-grams is SparkML's built-in `NGram` <a href="https://spark.apache.org/docs/latest/ml-features.html#n-gram" target="_blank">function</a>,  which takes an integer `n` and a list of tokens, and creates all possible groups of **exactly** `n` consecutive tokens.
# MAGIC 
# MAGIC Using SparkML's `NGram` function, fill in the following cell to return a new DataFrame, `ngramDF`, with the column, `ngrams`, containing all n-grams **up to and including** `n = 3`. In other words, append a single column which contains all the tokens (cleaned), bigrams, and trigrams of each corresponding row.
# MAGIC 
# MAGIC 
# MAGIC **Note:** We want to construct our n-grams using the `CleanTokens` column, *not* the raw `Tokens` column.
# MAGIC 
# MAGIC **Hint:** You may want to take a look at <a href = "https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.concat.html?highlight=concat#pyspark.sql.functions.concat" target="_blank"?> pyspark.sql.functions.concat</a>.

# COMMAND ----------

# ANSWER
# Apply n-grams to processed DataFrame
from pyspark.ml.feature import NGram
from pyspark.sql.functions import concat, col

ngram_df = processed_df.select("Text", "Tokens", "CleanTokens")

# Add bigram and trigram column
ngram2 = NGram(n=2, inputCol="CleanTokens", outputCol="ngrams2")
ngram3 = NGram(n=3, inputCol="CleanTokens", outputCol="ngrams3")

ngram_df = ngram2.transform(ngram_df)
ngram_df = ngram3.transform(ngram_df)

# Combine tokens, bigrams, and trigrams
ngram_df = ngram_df.withColumn("ngrams", concat(col("tokens"), col("ngrams2"), col("ngrams3")))

display(ngram_df.select("ngrams"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Similar to how we looked at the top tokens in our dataset, now we can use the `ngramDF` you created above to take a look at the most common n-grams (`n = 2` and `3`) in our dataset.

# COMMAND ----------

# Resulting top 25 ngrams
from pyspark.sql.functions import size, split, explode

ngram_dist = (ngram_df
              .withColumn("indivNGrams", explode(col("ngrams")))
              .filter(size(split("indivNGrams", " ")) > 1)  # only keep ngrams with n>1
              .groupBy("indivNGrams")
              .count()
              .sort(col("count").desc())
)

display(ngram_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC What do you notice about the frequent n-grams? Could they be important in text processing?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
