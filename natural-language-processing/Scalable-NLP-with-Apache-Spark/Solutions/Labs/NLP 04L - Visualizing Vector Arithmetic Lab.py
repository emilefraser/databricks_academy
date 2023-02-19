# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #  Visualizing Vector Arithmetic Lab
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC * Apply and visualize basic vector arithmetic to embeddings
# MAGIC * Calculate cosine similarity between vectors

# COMMAND ----------

# MAGIC %pip install gensim==4.0

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC We need the gensim library to load in the pretrained GloVe vectors again.

# COMMAND ----------

import gensim.downloader as api

# Load GloVe
word_vectors = api.load("glove-wiki-gigaword-100")

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to recreate the DataFrame and graph showcasing the embeddings of the words "man", "woman", "king", and "queen."

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

# Plot
def plot_vectors(vectors, words, title="Visualizing Word Embeddings", xlim1=-2, xlim2=5, ylim1=-0.5, ylim2=4.5):
  for coord, word in zip(vectors, words):
    plt.quiver(0, 0, coord[0], coord[1], angles="xy", scale_units="xy", scale=1)
    plt.text(coord[0] + 0.05, coord[1], word)
  plt.title(title)
  plt.xlim(xlim1, xlim2)
  plt.ylim(ylim1, ylim2)

plot_vectors(vectors, words)
plt.show()
plt.gcf().clear()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we want to be able to visualize the vector arithmetic involved with the `word_vectors.most_similar(positive=['woman', 'king'], negative=['man'])` call. The call will return the word with the embedding which is closest to the vector resulting from adding the embeddings in `positive` and subtracting the embeddings in `negative`.
# MAGIC 
# MAGIC To visualize what the resulting vector should be, we will continue to work with the reduced 2D representations of the pretrained GloVe embeddings and first add the positive embeddings `woman` and `king` before subtracting the embedding of `man`.
# MAGIC 
# MAGIC Fill in the following 2 lines to get
# MAGIC 1. the intermediate `woman + king` embedding
# MAGIC 2. the final `woman + king - man` embedding
# MAGIC 
# MAGIC and run the cell to plot the original 4 vectors with the 2 newly calculated vectors.

# COMMAND ----------

# ANSWER

# Plots 4 vectors from graph above
plot_vectors(vectors, words, "Visualizing 'woman+king-man'")

# Woman + king
w_plus_k = vectors[words.index("woman")] + vectors[words.index("king")]
# Woman + king - man
w_plus_k_minus_m = w_plus_k - vectors[words.index("man")]

# Formats new vectors and texts in graph
# w_plus_k
plt.quiver(
    0, 0, w_plus_k[0], w_plus_k[1], angles="xy", scale_units="xy", scale=1, color="blue"
)
plt.text(w_plus_k[0] + 0.1, w_plus_k[1], "woman+king")

# w_plus_k_minus_m
plt.quiver(
  w_plus_k[0],
  w_plus_k[1],
  w_plus_k_minus_m[0] - w_plus_k[0],
  w_plus_k_minus_m[1] - w_plus_k[1],
  angles="xy",
  scale_units="xy",
  scale=1,
  color="red",
)
plt.text(w_plus_k_minus_m[0] + 0.1, w_plus_k_minus_m[1] + 0.05, "(woman+king)-man")

plt.show()
plt.gcf().clear()

# COMMAND ----------

# MAGIC %md
# MAGIC Even though we reduced the embeddings down to 2 dimensions, what word does the resulting embedding still look the closest to? Does this match the answer that `word_vectors.most_similar(positive=['woman', 'king'], negative=['man'])` returns?
# MAGIC 
# MAGIC Note: Since vector addition and subtraction are commutative, you can change the order of the operations and see that the resulting `woman+king-man` vector still approximates the `queen` embedding.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we are going to explore what exactly "similar" embeddings mean.
# MAGIC 
# MAGIC Recall the definition of cosine similarity from above:
# MAGIC $$\text{CosineSimilarity }(u,v) = \cos(\text{ angle between vectors }) = \frac{u \cdot v}{|u| |v|}$$
# MAGIC 
# MAGIC Run the following cell to see what the `gensim` default function returns for the similarity between `queen` and `king`.

# COMMAND ----------

word_vectors.similarity("queen", "king")

# COMMAND ----------

# MAGIC %md
# MAGIC Now fill in the following `cos_similarity` function to calculate the cosine similarity between 2 vectors (np arrays) of equal dimensions.
# MAGIC 
# MAGIC Hint: Take a look at some numpy functions.

# COMMAND ----------

# ANSWER
import numpy as np

def cos_similarity(v1, v2):
  return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

word1 = "queen"
word2 = "king"
ans = cos_similarity(word_vectors[word1], word_vectors[word2])
print(ans)

assert round(ans, 3) == round(
    word_vectors.similarity(word1, word2), 3
), "Your answer does not match Gensim's"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the similarity measure that we implemented to be able to pick a word, out of an inputted list, that is "closest" to a given word.
# MAGIC 
# MAGIC Fill out the below function `most_similar` to return the word from `words` that has the closest cosine similarity to word `w`.

# COMMAND ----------

# ANSWER
import numpy as np

def most_similar(w, words):
  v = word_vectors[w]
  vecs = [word_vectors[word] for word in words]

  similarities = np.array([cos_similarity(v, vec) for vec in vecs])
  return words[np.argmax(similarities)]

most_similar("queen", ["king", "woman", "man", "princess"])

# COMMAND ----------

# MAGIC %md
# MAGIC You can play around with the inputs to `most_similar` and see which words the GloVe embedding model treats has having similar semantic meanings.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
