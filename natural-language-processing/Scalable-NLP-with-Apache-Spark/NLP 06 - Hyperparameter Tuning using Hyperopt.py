# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Hyperparameter Tuning using Hyperopt
# MAGIC 
# MAGIC The [Hyperopt library](https://github.com/hyperopt/hyperopt) allows for parallel hyperparameter tuning using either random search or Tree of Parzen Estimators (TPE). With MLflow, we can record the hyperparameters and corresponding metrics for each hyperparameter combination. You can read more on [SparkTrials w/ Hyperopt](https://github.com/hyperopt/hyperopt/blob/master/docs/templates/scaleout/spark.md).
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC - Tune the LSTM model we trained in the previous model using Hyperopt

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

from pyspark.sql.functions import col, when
import numpy as np
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pandas as pd
import mlflow.tensorflow
import mlflow
from hyperopt import fmin, hp, tpe, STATUS_OK, SparkTrials

# COMMAND ----------

text_df = (spark.read.parquet("/mnt/training/reviews/reviews_cleaned.parquet")
           .select("Text", "Score")
           .limit(5000) ### limit to only 5000 rows to reduce training time
          )
text_df = text_df.withColumn("sentiment", when(col("Score") > 3, 1).otherwise(0))
display(text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preprocessing
# MAGIC - Includes tokenization and padding

# COMMAND ----------

(train_df, test_df) = text_df.randomSplit([0.8, 0.2])
train_pdf = train_df.toPandas()
X_train = train_pdf["Text"].values
y_train = train_pdf["sentiment"].values

# COMMAND ----------

vocab_size = 10000
max_length = 500
tokenizer = Tokenizer(num_words=vocab_size)
tokenizer.fit_on_texts(X_train)

### Convert the texts to sequences.
X_train_seq = tokenizer.texts_to_sequences(X_train)
X_train_seq_padded = pad_sequences(X_train_seq, maxlen=max_length)

### Repeat for test_df 
test_pdf = test_df.toPandas()
X_test = test_pdf["Text"].values
y_test = test_pdf["sentiment"].values
X_test_seq = tokenizer.texts_to_sequences(X_test)
X_test_seq_padded = pad_sequences(X_test_seq, maxlen=max_length)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define LSTM Architecture

# COMMAND ----------

def create_lstm(hpo):
  from tensorflow.keras import layers ## Include imports due to TF serialization issues
  from tensorflow import keras
  ### Below is a slightly simplified architecture compared to the previous notebook to save time 
  embedding_dim = 64
  lstm_out = 32 
  
  inputs = keras.Input(shape=(None,), dtype="int32")
  x = layers.Embedding(vocab_size, embedding_dim)(inputs)
  x = layers.Bidirectional(layers.LSTM(lstm_out))(x)
  outputs = layers.Dense(1, activation="sigmoid")(x)
  model = keras.Model(inputs, outputs)

  return model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Hyperopt's objective function
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> We need to import `tensorflow` within the function due to a pickling issue.  <a href="https://docs.databricks.com/applications/deep-learning/single-node-training/tensorflow.html#tensorflow-2-known-issues" target="_blank">See known issues here.</a>

# COMMAND ----------

def run_lstm(hpo):
  ### Need to include the TF import due to serialization issues
  import tensorflow as tf
  
  model = create_lstm(hpo)
  
  ### Select Optimizer
  optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
  optimizer = optimizer_call(learning_rate=hpo["learning_rate"])

  ### Compile model 
  model.compile(optimizer, loss="binary_crossentropy", metrics=["AUC"])
  history =  model.fit(X_train_seq_padded, 
                       y_train, 
                       batch_size=32, 
                       epochs=int(hpo["num_epoch"]), 
                       validation_split=0.1)

  obj_metric = history.history["loss"][-1]
  return {"loss": obj_metric, "status": STATUS_OK}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define search space for tuning
# MAGIC 
# MAGIC We need to create a search space for HyperOpt and set up SparkTrials to allow HyperOpt to run in parallel using Spark worker nodes. We can also start a MLflow run to automatically track the results of HyperOpt's tuning trials.

# COMMAND ----------

space = {
  "num_epoch": hp.quniform("num_epoch", 1, 3, 1), 
  "learning_rate": hp.loguniform("learning_rate", np.log(1e-4), 0), ## max is 1 because exp(0) is 1, added np.log to prevent exp() resulting in negative values
  "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

spark_trials = SparkTrials(parallelism=2)

with mlflow.start_run() as run:
  best_hyperparam = fmin(fn=run_lstm, 
                         space=space, 
                         algo=tpe.suggest, 
                         max_evals=4, ### ideally this should be an order of magnitude larger than parallelism
                         trials=spark_trials,
                         rstate=np.random.RandomState(42))

best_hyperparam

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab
# MAGIC 
# MAGIC Try tuning other configurations!
# MAGIC - "lstm_out": hp.quniform("lstm_out", 20, 150, 1), 
# MAGIC - "embedding_dim": hp.quniform("embedding_dim", 40, 512, 1),

# COMMAND ----------

# TODO

### First modify the model 
def create_lstm(hpo):
  inputs = keras.Input(shape=(None,), dtype="int32")
  x = layers.Embedding(vocab_size, <# FILL_IN)(inputs)
  x = layers.Bidirectional(layers.LSTM(# FILL_IN, return_sequences=True))(x)
  x = layers.Bidirectional(layers.LSTM(# FILL_IN))(x)
  outputs = layers.Dense(1, activation="sigmoid")(x)
  model = keras.Model(inputs, outputs)
  return model

### Then modify the search space 
space = {
  "embedding_size": # FILL_IN,
  "lstm_size": # FILL_IN,
  "num_epoch": hp.quniform("num_epoch", 1, 5, 1),
  "learning_rate": hp.loguniform("learning_rate", np.log(1e-4), 0), ## max is 1 because exp(0) is 1, added np.log to prevent exp() resulting in negative values
  "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
