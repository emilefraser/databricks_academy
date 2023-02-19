# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_dlt_policy(self):
    from dbacademy.dbhelper import ClustersHelper

    dlt_policy = DA.client.cluster_policies.get_by_name(ClustersHelper.POLICY_DLT_ONLY)
    assert dlt_policy is not None, f"Could not find the cluster policy \"{ClustersHelper.POLICY_DLT_ONLY}\"; Please run the notebook Includes/Workspace-Setup before proceeding."
    
    return dlt_policy

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_pipeline_config(self):
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 9.1.3 - DLT Job"
    
    pipeline_name = f"DLT-Job-Demo-91-{DA.username}"
    
    return pipeline_name, path


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_pipeline_config(self):
    "Provided by DBAcademy, this function renders the configuration of the pipeline as HTML"
    from dbacademy.dbhelper import ClustersHelper

    pipeline_name, path = self.get_pipeline_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{pipeline_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{DA.schema_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Storage Location:</td>
        <td><input type="text" value="{DA.paths.storage_location}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook Path:</td>
        <td><input type="text" value="{path}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Datasets Path:</td>
        <td><input type="text" value="{DA.paths.datasets}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Policy:</td>
        <td><input type="text" value="{ClustersHelper.POLICY_DLT_ONLY}" style="width:100%"></td></tr>
    </table>""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_pipeline(self):
    "Provided by DBAcademy, this function creates the prescribed pipline"
    
    pipeline_name, path = self.get_pipeline_config()

    # We need to delete the existing pipline so that we can apply updates
    # because some attributes are not mutable after creation.
    self.client.pipelines().delete_by_name(pipeline_name)
    
    response = self.client.pipelines().create(
        name = pipeline_name, 
        storage = DA.paths.storage_location, 
        target = DA.schema_name, 
        notebooks = [path],
        configuration = {
            "spark.master": "local[*]",
            "datasets_path": DA.paths.datasets,
        },
        clusters=[{ 
            "num_workers": 0,
            "policy_id": self.get_dlt_policy().get("policy_id")
        }]
    )
    
    pipeline_id = response.get("pipeline_id")
    print(f"Created the pipeline \"{pipeline_name}\" ({pipeline_id})")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_pipeline_config(self):
    "Provided by DBAcademy, this function validates the configuration of the pipeline"
    import json
    from dbacademy.dbhelper import ClustersHelper
    
    pipeline_name, path = self.get_pipeline_config()

    pipeline = self.client.pipelines().get_by_name(pipeline_name)
    assert pipeline is not None, f"The pipline named \"{pipeline_name}\" doesn't exist. Double check the spelling."

    spec = pipeline.get("spec")
    
    storage = spec.get("storage")
    assert storage == DA.paths.storage_location, f"Invalid storage location. Found \"{storage}\", expected \"{DA.paths.storage_location}\" "
    
    target = spec.get("target")
    assert target == DA.schema_name, f"Invalid target. Found \"{target}\", expected \"{DA.schema_name}\" "
    
    libraries = spec.get("libraries")
    assert libraries is None or len(libraries) > 0, f"The notebook path must be specified."
    assert len(libraries) == 1, f"More than one library (e.g. notebook) was specified."
    first_library = libraries[0]
    assert first_library.get("notebook") is not None, f"Incorrect library configuration - expected a notebook."
    first_library_path = first_library.get("notebook").get("path")
    assert first_library_path == path, f"Invalid notebook path. Found \"{first_library_path}\", expected \"{path}\" "

    configuration = spec.get("configuration")
    assert configuration is not None, f"The two configuration parameters were not specified."
    datasets_path = configuration.get("datasets_path")
    assert datasets_path == DA.paths.datasets, f"Invalid datasets_path value. Found \"{datasets_path}\", expected \"{DA.paths.datasets}\"."
    spark_master = configuration.get("spark.master")
    assert spark_master == f"local[*]", f"Invalid spark.master value. Expected \"local[*]\", found \"{spark_master}\"."
    
    cluster_count = len(spec.get("clusters"))
    assert cluster_count == 1, f"Expected one, and only one, cluster configuration, found {cluster_count}. You can use the JSON UI to edit the configuration and remove the extra clusters."
    
    cluster = spec.get("clusters")[0]
    autoscale = cluster.get("autoscale")
    assert autoscale is None, f"Autoscaling should be disabled."
    
    num_workers = cluster.get("num_workers")
    assert num_workers == 0, f"Expected the number of workers to be 0, found {num_workers}."

    policy_id = cluster.get("policy_id")
    policy_name = None if policy_id is None else self.client.cluster_policies.get_by_id(policy_id).get("name")
    assert policy_id == self.get_dlt_policy().get("policy_id"), f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\", found \"{policy_name}\"."

    development = spec.get("development")
    assert development == True, f"The pipline mode should be set to \"Development\"."
    
    channel = spec.get("channel")
    assert channel is None or channel == "CURRENT", f"Expected the channel to be Current but found {channel}."
    
    photon = spec.get("photon")
    assert photon == True, f"Expected Photon to be enabled."
    
    continuous = spec.get("continuous")
    assert continuous == False, f"Expected the Pipeline mode to be \"Triggered\", found \"Continuous\"."

    print("All tests passed!")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_job_config(self):
    job_name = f"Jobs-Demo-91-{DA.username}"
    
    notebook = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook = "/".join(notebook.split("/")[:-1]) + "/DE 9.1.2 - Reset"

    return job_name, notebook


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_job_config_task_reset(self):
    "Provided by DBAcademy, this function renders the configuration of the job as HTML"
    job_name, reset_notebook = self.get_job_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Job Name:</td>
        <td><input type="text" value="{job_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Reset Notebook Path:</td>
        <td><input type="text" value="{reset_notebook}" style="width:100%"></td></tr>

    </table>""")    


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job_v1(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    
    job_name, reset_notebook = self.get_job_config()

    params = {
        "name": job_name,
        "tags": {
            "dbacademy.course": self.course_config.build_name,
            "dbacademy.source": self.course_config.build_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Reset",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": reset_notebook,
                    "base_parameters": []
                },
                "existing_cluster_id": dbgems.get_tags().get("clusterId")
            },
        ],
    }
    params = self.update_cluster_params(params, [0])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created the job \"{job_name}\" (#{job_id})")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_v1_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    job_name, reset_notebook = self.get_job_config()

    job = self.client.jobs.get_by_name(job_name)
    assert job is not None, f"The job named \"{job_name}\" doesn't exist. Double check the spelling."

    # print(json.dumps(job, indent=4))
    
    settings = job.get("settings")
    
    if settings.get("format") == "SINGLE_TASK":
        notebook_path = settings.get("notebook_task", {}).get("notebook_path")
        actual_cluster_id = settings.get("existing_cluster_id", None)
        #task_key = settings.get("task_key", None)
    else:
        tasks = settings.get("tasks", [])
        assert len(tasks) == 1, f"Expected one task, found {len(tasks)}."

        notebook_path = tasks[0].get("notebook_task", {}).get("notebook_path")
        actual_cluster_id = tasks[0].get("existing_cluster_id", None)
        
        task_key = tasks[0].get("task_key", None)
        assert task_key == "Rest", f"Expected the first task to have the name \"Reset\", found \"{task_key}\""
        
        
    assert notebook_path == reset_notebook, f"Invalid Notebook Path. Found \"{notebook_path}\", expected \"{reset_notebook}\" "
    
    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""
        
    print("All tests passed!")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job_v2(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    
    pipeline_name, path = self.get_pipeline_config()
    job_name, reset_notebook = self.get_job_config()

    self.client.jobs.delete_by_name(job_name, success_only=False)
    
    pipeline = self.client.pipelines().get_by_name(pipeline_name)
    pipeline_id = pipeline.get("pipeline_id")
    
    params = {
        "name": job_name,
        "tags": {
            "dbacademy.course": self.course_config.build_name,
            "dbacademy.source": self.course_config.build_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "Reset",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": reset_notebook,
                    "base_parameters": []
                },
                "existing_cluster_id": dbgems.get_tags().get("clusterId")
            },
            {
                "task_key": "DLT",
                "depends_on": [ { "task_key": "Reset" } ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
            },
        ],
    }
    params = self.update_cluster_params(params, [0])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created job #{job_id}")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_v2_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    pipeline_name, path = self.get_pipeline_config()
    job_name, reset_notebook = self.get_job_config()

    job = self.client.jobs.get_by_name(job_name)
    assert job is not None, f"The job named \"{job_name}\" doesn't exist. Double check the spelling."
    
    settings = job.get("settings")
    assert settings.get("format") == "MULTI_TASK", f"Expected two tasks, found 1."

    tasks = settings.get("tasks", [])
    assert len(tasks) == 2, f"Expected two tasks, found {len(tasks)}."
    
    
    # Reset Task
    task_name = tasks[0].get("task_key", None)
    assert task_name == "Reset", f"Expected the first task to have the name \"Reset\", found \"{task_name}\""
    
    notebook_path = tasks[0].get("notebook_task", {}).get("notebook_path")
    assert notebook_path == reset_notebook, f"Invalid Notebook Path for the first task. Found \"{notebook_path}\", expected \"{reset_notebook}\" "

    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        actual_cluster_id = tasks[0].get("existing_cluster_id", None)
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""

    
    
    # Reset DLT
    task_name = tasks[1].get("task_key", None)
    assert task_name == "DLT", f"Expected the second task to have the name \"DLT\", found \"{task_name}\""

    actual_pipeline_id = tasks[1].get("pipeline_task", {}).get("pipeline_id", None)
    assert actual_pipeline_id is not None, f"The second task is not configured to use a Delta Live Tables pipeline"
    
    expected_pipeline = self.client.pipelines().get_by_name(pipeline_name)
    actual_pipeline = self.client.pipelines().get_by_id(actual_pipeline_id)
    actual_name = actual_pipeline.get("spec").get("name", "Oops")
    assert actual_pipeline_id == expected_pipeline.get("pipeline_id"), f"The second task is not configured to use the correct pipeline, expected \"{pipeline_name}\", found \"{actual_name}\""
    
    depends_on = tasks[1].get("depends_on", [])
    assert len(depends_on) > 0, f"The \"DLT\" task does not depend on the \"Reset\" task"
    assert len(depends_on) == 1, f"The \"DLT\" task depends on more than just the \"Reset\" task"
    depends_task_key = depends_on[0].get("task_key")
    assert depends_task_key == "Reset", f"The \"DLT\" task doesn't depend on the \"Reset\" task, found {depends_task_key}"
    
    print("All tests passed!")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def start_job(self):
    job_name, reset_notebook = self.get_job_config()
    job_id = self.client.jobs.get_by_name(job_name).get("job_id")
    run_id = self.client.jobs.run_now(job_id).get("run_id")
    print(f"Started job #{job_id}, run #{run_id}")

    self.client.runs.wait_for(run_id)

# COMMAND ----------

# jobs_demo_91 is specifically referenced in the lesson
lesson_config.name = "jobs_demo_91"

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson() # First in a series
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

