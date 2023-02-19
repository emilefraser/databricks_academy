# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Smoke Tests 7.3
# MAGIC This notebook is provided solely for testing the Multi-Tasks jobs in section 7.3

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_databricks-academy-helper $lesson="8.4"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def get_job_config():
    """
    Returns the configuration to be used by the student in configuring the job.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-2]) + "/08 - Orchestration and Scheduling/ADE 8.4 - Deploying Workloads"
    
    da_name, da_hash = DA.get_username_hash()
    job_name = f"da-{da_name}-{da_hash}-{DA.course_code.lower()}: Streaming Job"
    
    return JobConfig(job_name, [
        TaskConfig(name="Reset-Pipelines",
                   resource_type="Notebook",
                   resource=f"{base_path}/1 - Reset Pipelines",
                   params={"batch_delay":"0"}),
        
        TaskConfig(name="Streaming-Job",
                   resource_type="Notebook",
                   resource=f"{base_path}/2 - Schedule Streaming Jobs",
                   depends_on=["Reset-Pipelines"],
                   params={"once":str(True)}),
        
        TaskConfig(name="Batch-Job",
                   resource_type="Notebook",
                   resource=f"{base_path}/3 - Schedule Batch Jobs",
                   depends_on=["Streaming-Job"],
                   params={"once":str(True)}),
        
        TaskConfig(name="Streaming-Progress",
                   resource_type="Notebook",
                   resource=f"{base_path}/4 - Streaming Progress",
                   depends_on=["Batch-Job"],
                   params={"once":str(True)}),
        
        TaskConfig(name="Conclusion",
                   resource_type="Notebook",
                   resource=f"{base_path}/5 - Demo Conclusion",
                   depends_on=["Streaming-Progress"]),
    ])
    
DA.print_job_config()

# COMMAND ----------

job_id = DA.create_job()

# COMMAND ----------

DA.start_job(job_id)

