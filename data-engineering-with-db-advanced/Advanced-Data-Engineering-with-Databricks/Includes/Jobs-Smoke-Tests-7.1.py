# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Smoke Tests 7.1
# MAGIC This notebook is provided solely for testing the Multi-Tasks jobs in section 7.1

# COMMAND ----------

# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %run ./_databricks-academy-helper $lesson="8.1"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def get_job_config():
    """
    Returns the configuration to be used by the student in configuring the job.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-2]) + "/08 - Orchestration and Scheduling/ADE 8.1 - Multi-Task Jobs"
    
    da_name, da_hash = DA.get_username_hash()
    job_name = f"da-{da_name}-{da_hash}-{DA.course_code.lower()}: Example Job"
    
    return JobConfig(job_name, [
        TaskConfig(name="Task-1",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-1, Create Database"),
        
        TaskConfig(name="Task-2",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-2, From Task 2",
                   depends_on=["Task-1"]),
        
        TaskConfig(name="Task-3",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-3, From Task 3",
                   depends_on=["Task-1"]),
        
        TaskConfig(name="Task-4",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-4, Key-Param",
                   depends_on=["Task-2", "Task-3"],
                   params={"name": "John Doe"}),
        
        TaskConfig(name="Task-5",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-5, Create task_5",
                   depends_on=["Task-1"]),
        
        TaskConfig(name="Task-6",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-6, Errors",
                   depends_on=["Task-5"]),
        
        TaskConfig(name="Task-7",
                   resource_type="Notebook",
                   resource=f"{base_path}/Task-7, Cleanup",
                   depends_on=["Task-4", "Task-6"]),
    ])
    
DA.print_job_config()

# COMMAND ----------

job_id = DA.create_job()

# COMMAND ----------

DA.start_job(job_id)

