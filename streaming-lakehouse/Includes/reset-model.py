# Databricks notebook source

from mlflow.tracking.client import MlflowClient
from mlflow.utils.rest_utils import RestException

dbutils.widgets.text("model_name", "foo")
model_name = dbutils.widgets.get("model_name")

client = MlflowClient()
try:
    for version in [model.version for stage in ["production", "staging"] for model in client.get_latest_versions(model_name, stages=[stage])]:
        client.transition_model_version_stage(model_name, version, "archived")
    client.delete_registered_model(model_name)
except RestException as E:
    print(E)

