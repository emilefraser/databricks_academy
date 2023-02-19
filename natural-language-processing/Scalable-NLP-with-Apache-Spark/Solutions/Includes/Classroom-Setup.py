# Databricks notebook source
import sys

# Suggested fix from Jason Kim & Ka-Hing Cheung
# https://databricks.atlassian.net/browse/ES-176458
wsfsPaths = list(filter(lambda p : p.startswith("/Workspace"), sys.path))
defaultPaths = list(filter(lambda p : not p.startswith("/Workspace"), sys.path))
sys.path = defaultPaths + wsfsPaths

spark.conf.set("com.databricks.training.module-name", "nlp")

# filter out warnings from python
# issue: https://github.com/RaRe-Technologies/smart_open/issues/319
import warnings
warnings.filterwarnings("ignore")

displayHTML("Preparing the learning environment...")

# COMMAND ----------

# MAGIC %run "./Class-Utility-Methods"

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

def init_mlflow_as_job():
  import mlflow
  job_experiment_id = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
      dbutils.entry_point.getDbutils().notebook().getContext().tags()
    )["jobId"]

  if job_experiment_id:
    mlflow.set_experiment(f"/Curriculum/Test Results/Experiments/{job_experiment_id}")
    
init_mlflow_as_job()

# COMMAND ----------

courseType = "il"
username = getUsername()
userhome = getUserhome()
workingDir = getWorkingDir(courseType).replace("_pil", "")

# COMMAND ----------

courseAdvertisements = dict()
courseAdvertisements["username"] = (
    "v",
    username,
    "No additional information was provided.",
)
courseAdvertisements["userhome"] = (
    "v",
    userhome,
    "No additional information was provided.",
)
courseAdvertisements["workingDir"] = (
    "v",
    workingDir,
    "No additional information was provided.",
)
allDone(courseAdvertisements)

