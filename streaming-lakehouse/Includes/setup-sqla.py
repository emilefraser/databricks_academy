# Databricks notebook source
# MAGIC 
# MAGIC %run ./data-source

# COMMAND ----------

import pyspark.sql.functions as F
import re

dbutils.widgets.text("course", "airbnb")
course = dbutils.widgets.get("course")

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_demo_db"""
prefix = re.sub("[^a-zA-Z0-9]", "_", username.split("@")[0])
model_name=f"{prefix}_airbnb_rf"

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "reset")
mode = dbutils.widgets.get("mode")

if mode == "reset":

    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
    
    dbutils.notebook.run("./Includes/reset-model", 60, {"model_name":model_name})

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

filepath = f"{userhome}/bronze_chicago"

spark.sql(f"""
  CREATE TABLE bronze_chicago
  DEEP CLONE delta.`{URI}/bronze_chicago`
  LOCATION '{filepath}'
""")

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, reset=True, max_batch=6):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.silver = demohome+ "/silver"
        self.batch = 0
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.silver, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("file_alias") == self.batch)
                    .drop("file_alias")
                    .write
                    .mode("append")
                    .format("delta")
                    .option("path", self.silver)
                    .saveAsTable("silver_chicago"))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("file_alias") == self.batch)
                .drop("file_alias")
                .write
                .mode("append")
                .format("delta")
                .option("path", self.silver)
                .saveAsTable("silver_chicago"))
            self.batch += 1

# COMMAND ----------

File = FileArrival(userhome)
File.arrival()


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

File.arrival()
File.arrival()
train_model(model_name)
promote_latest()
train_model(model_name, batch_date="20201024")
promote_latest(target_stage="staging")

# COMMAND ----------

stream_predictions()
stream_predictions(model_stage="staging")

