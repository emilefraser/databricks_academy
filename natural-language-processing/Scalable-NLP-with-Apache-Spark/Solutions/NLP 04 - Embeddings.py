# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Embeddings
# MAGIC 
# MAGIC This lesson introduces the notion of a word embedding and explores tools to create and use them.
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Learn the motivation behind embeddings
# MAGIC * Train your own Word2Vec model using SparkML
# MAGIC * Use principal components analysis to visualize word embeddings
# MAGIC * Load a pretrained GloVe model using Gensim
# MAGIC * Apply and visualize basic vector arithmetic to embeddings

# COMMAND ----------

# MAGIC %pip install gensim==4.0

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Word Embedding?
# MAGIC 
# MAGIC A **word embedding** is a mapping of words to vectors in a higher dimensional space. This mapping has words with similar meanings mapped close to each other in the vector space while different words lie far apart from each other. In this way, the topography of this vector space approximates the semantic "space" of word meanings in natural language. Once we have mapped words into this semantic vector space, we can manipulate the vector representation of a word to get to a different vector which represents a different word.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/Word-Vectors.png )
# MAGIC 
# MAGIC From this image of words this higher dimensional space, we can see that the embeddings are able to capture the relationship between "male" and "female", and "country" and "capital" as well as the concept of verb tenses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latent Semantic Analysis: the old way to get word-vectors
# MAGIC 
# MAGIC An older technique for obtaining semantic word vectors was to perform Latent Semantic Analysis (LSA) on a corpus of text. This involved building up a document-term matrix where the rows represent documents, and the columns represent terms. The values of a given cell might be TF-IDF scores for that term in that document. Then, you peform a principal components analysis (PCA) on this matrix to obtain a mapping from the terms into a more compact semantic space. This is relatively easy to do, but has a number of drawbacks:
# MAGIC * it can be very computationally expensive to represent and factorize very large matrices (even with a distributed engine like Spark) especially on the large text corpora needed for good NLP performance
# MAGIC * PCA is limited to *linear* transformations, which does not seem to do a good job of mapping words to their various meanings (especially with words that have different meanings in different contexts)
# MAGIC 
# MAGIC For the above reasons, people have mostly turned away from LSA to various neural network models in order to get efficient embeddings. It can still be useful to do, occasionally, especially for relatively small text corpora and as a first approximation for bootstrapping other NLP processes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train a Word2Vec Model using SparkML
# MAGIC 
# MAGIC There are various different ways of obtaining these vector representations of words. Below are a few commonly used embedding models:
# MAGIC * <a href="https://code.google.com/archive/p/word2vec/Word2Vec" target="_blank"> Word2Vec </a> by Google
# MAGIC * <a href="https://nlp.stanford.edu/projects/glove/" target="_blank"> GloVe </a> by Stanford NLP
# MAGIC * <a href="https://fasttext.cc/" target="_blank"> fastText </a> by Facebook
# MAGIC * <a href="https://allennlp.org/elmo" target="_blank"> ELMO </a> by Allen NLP
# MAGIC * <a href="https://bert-embedding.readthedocs.io/en/latest/"> BERT </a> by Google
# MAGIC 
# MAGIC There are also different libraries which we can easily train/load word embeddings with:
# MAGIC * <a href="https://spacy.io/api/vectors" target="_blank"> spaCy </a> has built in vectors
# MAGIC * <a href="https://radimrehurek.com/gensim/index.html" target="_blank"> Gensim </a> lets you train your own models and load in pre-trained models
# MAGIC * <a href="https://spark.apache.org/docs/latest/ml-features.html#word2vec" target="_blank"> SparkML </a> lets you train models in parallel
# MAGIC 
# MAGIC In this section we will focus on using SparkML to train a custom embedding model specific to our reviews dataset.
# MAGIC 
# MAGIC Within SparkML, there is built-in functionality to learn a distributed vector representation of words using the Word2Vec approach. Given a column of text, the `Word2Vec` function will assign to each row its respective word embedding. For our reviews DataFrame, this means each review will be given one vector representation, the average embedding of all words in the review. More details about this training process can be found at this <a href="https://spark.apache.org/docs/latest/ml-features.html#word2vec" target="_blank"> SparkML Word2Vec </a> documentation page.
# MAGIC 
# MAGIC Run the following cell to obtain Word2Vec embeddings of `Text` in our reviews dataframe.

# COMMAND ----------

processed_df = spark.read.parquet("/mnt/training/reviews/tfidf.parquet")
display(processed_df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Word2Vec: SparkML
# MAGIC 
# MAGIC When training our embeddings, in SparkML's Word2Vec function we can specify two hyperparameters, `vectorSize` and `minCount`.
# MAGIC * `vectorSize` is the dimension of the embeddings that we want to learn and typically we want this dimension to be lot smaller than the number of unique words in our dataset (benefit over TFIDF and one-hot encoded vectors).
# MAGIC * `minCount` is the minimum number of times a unique word must appear to be included in the Word2Vec model's vocabulary. This hyperparameter can be used to reduce the amount of computation done by not training our model on every single word that appears.
# MAGIC 
# MAGIC Run this cell to train our Word2Vec model and obtain embeddings for each row of text in our DataFrame. The SparkML model will learn a vector representation for each unique token in our dataset so the single vector that represents each row of text is simply the average of all the word embeddings in the text.
# MAGIC 
# MAGIC Note: We are only training with 10,000 rows of text to save time.

# COMMAND ----------

from pyspark.ml.feature import Word2Vec

# Learn a vector representation for each row of text
word_2_vec = Word2Vec(vectorSize=20, minCount=2, inputCol="CleanTokens", outputCol="word2vecEmbedding")
model = word_2_vec.fit(processed_df.limit(10000))

wv_df = model.transform(processed_df.limit(10000))
display(wv_df.select("Text", "word2vecEmbedding").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Pre-Trained Model Using Gensim
# MAGIC 
# MAGIC When we train our own embedding representation, we are building a domain-specific model versus using a pretrained off-the-shelf model. Since a good embedding model requires large amounts of training data, it is often better to load and use a pretrained model. This is especially relevant if your text does not contain a lot of domain specific words, and if the amount of text that you have available to train on is limited.
# MAGIC 
# MAGIC Another technique we can use with a pre-trained model is transfer learning. By using a previously trained model as the starting point of your model instead of training everything from scratch, transfer learning can save you a lot of computational and time resources.
# MAGIC 
# MAGIC Our reviews dataset probably does not have much, if any, domain specific words and is likely not large enough for a trained model to perform well. So for a more comprehensive and generalizable model, in the following cell, we will load and use the pretrained GloVe model through the Gensim API. The GloVe model we are using was trained using Wikipedia 2014 and Gigaword 5 data, with 400,000 tokens as unique 100D vectors.

# COMMAND ----------

import gensim.downloader as api

# Load GloVe
word_vectors = api.load("glove-wiki-gigaword-100")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the embedding of the word `california`.

# COMMAND ----------

california_vec = word_vectors.get_vector("california")
print("Embedding dimension:", california_vec.shape)
print("Vector embedding for the token 'california':\n", california_vec)

# COMMAND ----------

# MAGIC %md
# MAGIC A distance metric called **cosine similarity** is used to determine how "close" 2 vectors are to each other.
# MAGIC 
# MAGIC Given 2 vectors u and v,
# MAGIC $$\text{CosineSimilarity }(u,v) = \cos(\text{ angle between vectors }) = \frac{u \cdot v}{|u| |v|}$$
# MAGIC 
# MAGIC This metric ranges from -1 to 1, with a value of 1 indicating that the words are exactly the same, a value of -1 indicating the words are exactly opposite, and a cosine similarity of 0 representing orthogonality between the words (they are unrelated). We can use intermediate values as an indicator of how "close" 2 words are to each other given their word embedding representations.

# COMMAND ----------

# MAGIC %md
# MAGIC Using vector arithmetic and the cosine similarities between the 400,000 tokens, Gensim will give us word suggestions. It looks through all 400,000 tokens of the model and returns those whose embeddings have the closest cosine similarity to the embedding of `california`.

# COMMAND ----------

print("Words with vectors most similar to 'california':")
word_vectors.most_similar("california")

# COMMAND ----------

# MAGIC %md
# MAGIC Using the `positive` and `negative` parameters of the `.most_similar()` function, we will get words with embeddings that are most similar to the `positive` embeddings while having the `negative` embeddings subtracted from it.
# MAGIC 
# MAGIC Since these embeddings are trying to capture the semantic meaning behind tokens, we can think of the following command as combining the concepts of "woman" and "king" and subtracting from that resulting combination the concept of a "man".

# COMMAND ----------

result = word_vectors.most_similar(positive=["woman", "king"], negative=["man"])
result

# COMMAND ----------

# MAGIC %md
# MAGIC Unsurprisingly the most similar word is "queen". This word encapsulates the meaning of "king" if you were to remove the meaning of "man" and replace it with the meaning of "woman".
# MAGIC That is to say that the difference in meaning between "king" and "queen" is the same as the difference in meaning between "man" and "woman" (ie. gender).
# MAGIC 
# MAGIC If you've ever done a standardized test (such as the GRE or the SAT), you may recognize this as a linguistic "analogy" problem:
# MAGIC 
# MAGIC $$man : woman$$
# MAGIC $$king : X$$
# MAGIC 
# MAGIC >_"man" is to "woman" as "king" is to what word?_
# MAGIC 
# MAGIC The fact that we can solve these relatively sophisticated analogy problems with just vector arithmetic shows that these neural embeddings are capturing the some of the essential "structure" of natural language. We can think of the vector space described by these embeddings as homologous to the semantic space of meanings in human language, and sometimes these embeddings are referred to as "semantic spaces".

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to get a better understanding of how the embeddings work by visualizing the embeddings of `woman`, `king`,  `man`, and `queen`.
# MAGIC 
# MAGIC Run the cell below to create a DataFrame containing the 100D embeddings generated by GloVE and mapped 2D embeddings of the words `woman`, `king`,  `man`, `queen`, and `object`. Let's graph the PCA results of our 4 vectors, `woman`, `king`,  `man`, and `queen`, in a 2D graph.

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors

def get_embed_df(words):
  # Create df
  vecs = [Vectors.dense([val.item() for val in word_vectors[word]]) for word in words]
  df_list = [(i, word, vec) for i, (word, vec) in enumerate(zip(words, vecs))]
  df = spark.createDataFrame(df_list, ["id", "word", "vectors"])

  # Reduce to 2 dim for plotting
  pca = PCA(k=2, inputCol="vectors", outputCol="2d_vectors")
  model = pca.fit(df)
  return model.transform(df)


words = ["man", "woman", "king", "queen", "object"]
embed_df = get_embed_df(words)

# Get the 4 vectors we are interested in
vectors = [row[0] for row in embed_df.select("2d_vectors").collect()[:4]]

def plot_vectors(vectors, words):
  for coord, word in zip(vectors, words):
    plt.quiver(0, 0, coord[0], coord[1], angles="xy", scale_units="xy", scale=1)
    plt.text(coord[0] + 0.05, coord[1], word)
    plt.title("Visualizing Word Embeddings")
    plt.xlim(-2, 5)
    plt.ylim(-0.5, 4.5)


plot_vectors(vectors, words)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try some more examples.

# COMMAND ----------

examples = [
    ["ice", "cold", "heat"],
    ["ocean", "water", "sand"],
    ["walking", "walked", "swam"],
    ["rode", "ride", "drive"],
    ["london", "england", "france"],
    ["rome", "italy", "japan"],
    ["aunt", "woman", "man"],
    ["dog", "man", "woman"],
    ["doctor", "man", "woman"],
]

for start, neg, pos in examples:
    print(
        f"{start} - {neg} + {pos} = {word_vectors.most_similar(positive=[start, pos], negative=[neg])[0][0]}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implicit bias in word vectors
# MAGIC 
# MAGIC Do all the above examples make sense to you? As you can see, these vectors span many semantic dimensions, not only gender. We can other meaningful attributes here, such as: heat-cold, dry-wet, past-present, as well as the capital cities of several countries.
# MAGIC 
# MAGIC There are some strange examples at the end, however. Returning to gender, we can see a few odd effects here. While gender is part of the definition of "aunt" and "uncle" (just as it is for "king" and "queen"), it is peculiar that gender is involved at all in the distinction between "dog" and "cat", and it is just wrong that it should be in the representation for "doctor" and "nurse".
# MAGIC 
# MAGIC What is going on here?
# MAGIC 
# MAGIC Word embeddings are trained on very large corpora of text, usually taken from the public internet (the GloVE model was trained on Wikipedia articles). Unfortunately, there is still a fair bit of bias in the world around issues such as gender and race, and these biases will be present in the language for some of these articles. The neural networks that learn the word embeddings will pick up this bias in just the same way they pick up on all of other ways in which humans make distinctions between words and meanings. Thus, in addition to pure semantic meaning, the model also learns and applies the biases and stereotypes present within the training data. Some of these biases may be idiosyncratic and culture-specific (such as identifying dogs as masculine and cats as feminine), but others can be quite harmful.
# MAGIC 
# MAGIC Researchers are continuing to develop ways of identifying and "de-biasing" or correcting for such biases in language models, and this remains an area of active research (see [here](https://papers.nips.cc/paper/6228-man-is-to-computer-programmer-as-woman-is-to-homemaker-debiasing-word-embeddings.pdf)). It is important to be aware of these implicit biases in our data, and try to correct them, because otherwise A.I. systems we build on it will simply continue perpetuate these biases in future decisions that they make. This can be both embarrassing and costly to companies that deploy such models for real-world use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic Search
# MAGIC 
# MAGIC One simple application for semantic similarity is as a filter for finding related items in a text corpus. To start, build a target vector by adding together a number of related terms that relate to the search topic of interest, the more the better. Any single word vector may contain elements representing alternative meanings (polysemy), so adding (or averaging) together a number of word vectors will build a target vector that is more focused only on the shared / common attributes.
# MAGIC 
# MAGIC **Note:** that since vector cosines are a measure of the angle between two vectors, the absolute length of the vectors (the norm) does not matter in semantic similarity - only the directions in which they point.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's search for product reviews that describe delicious foods. First, we set up a target vector based on related terms that together describe what we're looking for...

# COMMAND ----------

import numpy as np

keywords = ["food", "eat", "flavor", "yummy", "delicious", "taste", "tasty", "sweet", "salty", "spicy", "savory", "drink", "snack"]

target_vector = np.sum(np.array([word_vectors.get_vector(w) for w in keywords]), axis=0).tolist()
target_vector

# COMMAND ----------

# MAGIC %md
# MAGIC Here's the product review data we previously cleaned and prepared

# COMMAND ----------

display(processed_df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC Now build a vector of the words in the description. Since `pandas_udf` does not currently support `VectorUDT` type, we will be using regular `udf`.

# COMMAND ----------

from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import col

@udf(returnType=VectorUDT())
def tokens_2_vector(tokens):
  desc_vec = np.zeros(100)
  for token in tokens:
    try:
      desc_vec += word_vectors.get_vector(token)
    except KeyError:
      continue
  return Vectors.dense(desc_vec)

vectorized_df = processed_df.select("Id",
                                    "ProductId",
                                    "Score",
                                    "Summary",
                                    "Text",
                                    "CleanTokens",
                                    tokens_2_vector(col("CleanTokens")).alias("Vector"))

display(vectorized_df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we compute semantic similarity (ie. cosine) of the review vector to our target vector.
# MAGIC Sorting by decreasing similarity scores should display the most relevant results at the top of the list.

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import array, lit
from numpy.linalg import norm

@udf(returnType=DoubleType())
def similarity(a, b):
  
  a_norm = norm(a)
  b_norm = norm(b)
  similarity = float(np.dot(a, b) / (a_norm * b_norm)) if (a_norm != 0.0 and b_norm != 0.0) else 0.0
  
  return similarity

sim_df = vectorized_df.withColumn(
    "SimilarityToTarget",
    similarity(col("Vector"), array([lit(x) for x in target_vector])),
)

display(sim_df
        .select("Id", "Summary", "ProductId", "Text", "SimilarityToTarget")
        .orderBy(col("SimilarityToTarget").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see above, the top reviews returned by this query are all ones that describe tasty or delicious foods - exactly what we were searching for!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
