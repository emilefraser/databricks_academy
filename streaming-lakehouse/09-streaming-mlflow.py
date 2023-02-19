# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming with MLflow
# MAGIC 
# MAGIC In this notebook, we'll demonstrate using MLflow Model Registry as part of a Structured Streaming pipeline. This notebook seeks to empower data engineers and ML engineers to understand how MLflow can easily deploy arbitrary models to make near-real-time predictions on streaming data. 
# MAGIC 
# MAGIC **NOTE**: this notebook is **not** designed to teach all the intricacies of MLflow or anything about data science or machine learning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC The linked notebook:
# MAGIC 1. cleans up the environment used by this notebook
# MAGIC 1. defines logic and paths that will be used in this notebook
# MAGIC 1. defines a class we'll use to trigger new data arriving into our `silver_chicago` table

# COMMAND ----------

# MAGIC %run ./Includes/setup-streaming-mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC Our data has already been prepped for local modeling with sklearn. The fields `id` and `batch_date` serve as our composite key; we can see that all other fields will be loaded into Pandas as floats using the code below.

# COMMAND ----------

spark.table("silver_chicago").toPandas().dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Here, the ML team has written a simple function to train a random forest model on a given batch of data from the `silver_chicago` table.
# MAGIC 
# MAGIC Note that the final block of this code will add the model to the MLflow Model Registry.
# MAGIC 
# MAGIC Because we'll be interacting with our model through MLflow, we don't return anything from our function.

# COMMAND ----------

def train_model(model_name, batch_date="20200830", max_depth=100, n_estimators=100):
    import mlflow
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import make_scorer, mean_squared_error
    
    pdf = spark.table("silver_chicago").filter(F.col("batch_date") == batch_date).toPandas().set_index(["id", "batch_date"])
    
    X_train, X_test, y_train, y_test = train_test_split(
        pdf.drop(["price"], axis=1),
        pdf["price"],
        test_size=0.2,
        random_state=42
    )
    
    with mlflow.start_run() as run:

        # Train model on entire training data
        regressor = RandomForestRegressor(max_depth=max_depth, n_estimators=n_estimators, random_state=42)
        regressor.fit(X_train, y_train)

        # Evaluator on train and test set
        train_rmse = mean_squared_error(y_train, regressor.predict(X_train), squared=False)
        test_rmse = mean_squared_error(y_test, regressor.predict(X_test), squared=False)

        # Log parameters and metrics
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_metric("loss", test_rmse)

        # Register model
        mlflow.sklearn.log_model(
            sk_model = regressor, 
            artifact_path = "sklearn-model",
            registered_model_name=model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll train a simple model using the default parameters.
# MAGIC 
# MAGIC After model training, you should see a message about successfully registering a new model and creating version 1.

# COMMAND ----------

train_model(model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC We can manage our registered models using the GUI, or programmatically.
# MAGIC 
# MAGIC The code in the following cell will move the newest version of our model into the `production` stage.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()

def promote_latest(target_stage="production"):
    latest_version = max([model.version for model in client.get_registered_model(model_name).latest_versions])

    client.transition_model_version_stage(
        name=model_name,
        version=latest_version,
        stage=target_stage,
        archive_existing_versions=True    
    )

promote_latest()

# COMMAND ----------

# MAGIC %md
# MAGIC The first block of code in our function below loads in our model from the MLflow registry as a Spark UDF. As such, we can apply this function as a transformation in any DataFrame operation, including when working with streaming data. This means our single-node model can not be distributed to make predictions using all nodes available in our current cluster.
# MAGIC 
# MAGIC The other code in this function does a bit of data manipulation to set up our data for predictions, and then creates a target table to write our stream into.

# COMMAND ----------

def stream_predictions(model_stage="production"):
    import mlflow

    # Load model as a Spark UDF.
    loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{model_stage}")
    
    # Load source data
    df = spark.readStream.table("silver_chicago")
    
    # Prepare data for predictions
    columns = df.drop("price").columns[2:]
    
    # Apply the model and select the desired output
    predDF = df.select("id", "batch_date", "price", loaded_model(*columns).alias("predicted"))
    
    # Register a target Delta table
    schema = predDF._jdf.schema().toDDL()
    spark.sql(f"CREATE TABLE IF NOT EXISTS preds_{model_stage} ({schema}) USING delta LOCATION '{userhome}/preds_{model_stage}'")
              
    # Write predictions as a stream          
    predDF.writeStream.format("delta").option("checkpointLocation", f"{userhome}/checkpoint_preds_{model_stage}").table(f"preds_{model_stage}")

# COMMAND ----------

# MAGIC %md
# MAGIC When we call our function, we'll essentially be pushing our model into production (assuming the table we're writing to represent our production data).

# COMMAND ----------

stream_predictions()

# COMMAND ----------

# MAGIC %md
# MAGIC Our code is configured to append new predictions; the cell below will land another batch of records and we'll see new predictions appended to our target table.

# COMMAND ----------

File.arrival()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM preds_production
# MAGIC ORDER BY batch_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Note our queries are static, but will always reflect the newest version of our Delta tables.

# COMMAND ----------

File.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our predictions are changing over time because the features associated with each listing ID are changing from month-to-month.
# MAGIC 
# MAGIC Are these predictions better or worse than the original predictions from August? **This is someone else's job to determine.** Our job is to keep the desired model in production.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id, price, a.predicted oct_pred, b.sept_pred
# MAGIC FROM preds_production a
# MAGIC INNER JOIN (SELECT id, predicted sept_pred FROM preds_production WHERE batch_date = '20200921') b
# MAGIC ON a.id = b.id
# MAGIC WHERE batch_date = '20201024'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train a New Model
# MAGIC 
# MAGIC Here, the DS/ML team has decided to train a new model using the data from October.

# COMMAND ----------

train_model(model_name, batch_date="20201024")

# COMMAND ----------

# MAGIC %md
# MAGIC Rather than pushing this model directly to production, they've decided to run it in a staging environment so they can compare results.

# COMMAND ----------

promote_latest(target_stage="staging")

# COMMAND ----------

# MAGIC %md
# MAGIC By deploying a second streaming pipeline using this staging model, we can empower analysts and data scientists access to predictions from both.

# COMMAND ----------

stream_predictions(model_stage="staging")

# COMMAND ----------

# MAGIC %md
# MAGIC Now when new data arrives, both our staging and production prediction tables will be updated.

# COMMAND ----------

File.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC We can visually inspect our results to confirm that different predictions are being made.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id, a.batch_date, a.price, a.predicted prod_pred, b.predicted stage_pred
# MAGIC FROM preds_production a
# MAGIC INNER JOIN preds_staging b
# MAGIC ON a.id = b.id AND a.batch_date = b.batch_date
# MAGIC ORDER BY batch_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Our data science team has now created two models. The first was trained using August listing data, while the second was trained on data from October. In reality, data scientists may have dozens of models with different algorithms and hyperparameters to compare. Here, we won't worry about determining model quality and just trust our data science team.
# MAGIC 
# MAGIC If the data scientists decide the new model is better, all they need to do is promote the model stage to `production`.

# COMMAND ----------

promote_latest(target_stage="production")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that this change is not immediate. While our logic for our stream now points to a model URI for an updated model, our stream was initialized with a physical plan that leveraged a UDF from our old model.
# MAGIC 
# MAGIC The new model will be applied when the stream restarts. The cell below will trigger this restart.

# COMMAND ----------

stream_predictions(model_stage="production")

# COMMAND ----------

# MAGIC %md
# MAGIC When we make new predictions, we should be able to see that the same algorithm is being applied to both our production and staging streams.

# COMMAND ----------

File.arrival()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id, a.batch_date, a.price, a.predicted prod_pred, b.predicted stage_pred
# MAGIC FROM preds_production a
# MAGIC INNER JOIN preds_staging b
# MAGIC ON a.id = b.id AND a.batch_date = b.batch_date
# MAGIC ORDER BY batch_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the new model is only being applied to newly arriving batches of data; if we look back at previous months, we'll still have our older model's predictions.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id, a.batch_date, a.price, a.predicted prod_pred, b.predicted stage_pred
# MAGIC FROM preds_production a
# MAGIC INNER JOIN preds_staging b
# MAGIC ON a.id = b.id AND a.batch_date = b.batch_date
# MAGIC WHERE a.batch_date = 20201108
# MAGIC ORDER BY batch_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC As always, stop your streams before exiting the notebook.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## More References
# MAGIC 1. [Databricks Academy](https://academy.databricks.com/) has training on MLflow, data science, and machine learning engineering
# MAGIC 1. [MLflow Model Serving](https://databricks.com/blog/2020/06/25/announcing-mlflow-model-serving-on-databricks.html) provides API endpoints for online model predictions
# MAGIC 1. [MLflow Documentation](https://www.mlflow.org/docs/latest/index.html) is maintained as part of this open source project

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
