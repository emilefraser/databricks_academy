# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_sql(self, rows, sql):
    displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")
    

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_daily_patient_avg(self):
    sql = f"SELECT * FROM {DA.schema_name}.daily_patient_avg"
    self.print_sql(3, sql)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_visualization_query(self):
    sql = f"""
SELECT flow_name, timestamp, int(details:flow_progress:metrics:num_output_rows) num_output_rows
FROM {DA.schema_name}.dlt_metrics
ORDER BY timestamp DESC;"""
    
    self.print_sql(5, sql)

# COMMAND ----------

generate_register_dlt_event_metrics_sql_string = ""

@DBAcademyHelper.monkey_patch
def generate_register_dlt_event_metrics_sql(self):
    global generate_register_dlt_event_metrics_sql_string
    
    generate_register_dlt_event_metrics_sql_string = f"""
CREATE TABLE IF NOT EXISTS {DA.schema_name}.dlt_events
LOCATION '{DA.paths.working_dir}/storage/system/events';

CREATE VIEW IF NOT EXISTS {DA.schema_name}.dlt_success AS
SELECT * FROM {DA.schema_name}.dlt_events
WHERE details:flow_progress:metrics IS NOT NULL;

CREATE VIEW IF NOT EXISTS {DA.schema_name}.dlt_metrics AS
SELECT timestamp, origin.flow_name, details 
FROM {DA.schema_name}.dlt_success
ORDER BY timestamp DESC;""".strip()
    
    self.print_sql(13, generate_register_dlt_event_metrics_sql_string)
    

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
    notebook = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    notebook = "/".join(notebook.split("/")[:-1]) + "/DE 12.2.2L - DLT Task"
    
    job_name = f"Cap-12-{DA.username}"
    
    return job_name, notebook


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
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{DA.paths.stream_path}" style="width:100%"></td></tr>
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
        storage = self.paths.storage_location, 
        target = self.schema_name, 
        notebooks = [path],
        continuous = True,
        development = False,
        configuration = {
            "spark.master": "local[*]",
            "datasets_path": DA.paths.datasets,
            "source": DA.paths.stream_path,
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
    assert configuration is not None, f"The three configuration parameters were not specified."
    datasets_path = configuration.get("datasets_path")
    assert datasets_path == DA.paths.datasets, f"Invalid datasets_path value. Found \"{datasets_path}\", expected \"{DA.paths.datasets}\"."
    stream_source = configuration.get("source")
    assert stream_source == DA.paths.stream_path, f"Invalid \"source\" value. Expected \"{DA.paths.stream_path}\", found \"{stream_source}\"."

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
    assert development == False, f"The pipline mode should be set to \"Production\"."
    
    channel = spec.get("channel")
    assert channel is None or channel == "CURRENT", f"Expected the channel to be Current but found {channel}."
    
    photon = spec.get("photon")
    assert photon == True, f"Expected Photon to be enabled."
    
    continuous = spec.get("continuous")
    assert continuous == True, f"Expected the Pipeline mode to be \"Continuous\", found \"Triggered\"."

    print("All tests passed!")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_job_config(self):
    
    root = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    root = "/".join(root.split("/")[:-1])
    
    notebook = f"{root}/DE 12.2.3L - Land New Data"

    job_name = f"Cap-12-{DA.username}"
    
    return job_name, notebook

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_job_config(self):
    
    job_name, notebook = self.get_job_config()

    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Job Name:</td>
        <td><input type="text" value="{job_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook Path:</td>
        <td><input type="text" value="{notebook}" style="width:100%"></td></tr>
    </table>""")

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_job(self):
    "Provided by DBAcademy, this function creates the prescribed job"
    
    job_name, notebook = self.get_job_config()

    self.client.jobs.delete_by_name(job_name, success_only=False)
    cluster_id = dbgems.get_tags().get("clusterId")
    
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
                "task_key": "Land-Data",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": notebook,
                    "base_parameters": []
                },
                "existing_cluster_id": cluster_id
            },
        ],
    }
    params = self.update_cluster_params(params, [0])
    
    create_response = self.client.jobs().create(params)
    job_id = create_response.get("job_id")
    
    print(f"Created job #{job_id}")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_job_config(self):
    "Provided by DBAcademy, this function validates the configuration of the job"
    import json
    
    pipeline_name, job_path = self.get_pipeline_config()
    job_name, notebook = self.get_job_config()

    job = self.client.jobs.get_by_name(job_name)
    assert job is not None, f"The job named \"{job_name}\" doesn't exist. Double check the spelling."
    
    settings = job.get("settings")
    assert settings.get("format") == "SINGLE_TASK", f"""Expected only one task."""

    # Land-Data Task
#     task_name = settings.get("task_key", None)
#     assert task_name == "Land-Data", f"Expected the first task to have the name \"Land-Data\", found \"{task_name}\""
    
    notebook_path = settings.get("notebook_task", {}).get("notebook_path")
    assert notebook_path == notebook, f"Invalid Notebook Path for the first task. Found \"{notebook_path}\", expected \"{notebook}\" "

    if not self.is_smoke_test():
        # Don't check the actual_cluster_id when running as a smoke test
        
        actual_cluster_id = settings.get("existing_cluster_id", None)
        assert actual_cluster_id is not None, f"The first task is not configured to use the current All-Purpose cluster"

        expected_cluster_id = dbgems.get_tags().get("clusterId")
        if expected_cluster_id != actual_cluster_id:
            actual_cluster = self.client.clusters.get(actual_cluster_id).get("cluster_name")
            expected_cluster = self.client.clusters.get(expected_cluster_id).get("cluster_name")
            assert actual_cluster_id == expected_cluster_id, f"The first task is not configured to use the current All-Purpose cluster, expected \"{expected_cluster}\", found \"{actual_cluster}\""

    schedule = settings.get("schedule")
    if schedule is None:
        print("WARNING: The job has not been scheduled.\n")
    else:
        pause_status = schedule.get("pause_status")
        if pause_status == "PAUSED":
            print("WARNING: The job should not be paused.\n")
        else:
            quartz_cron_expression = schedule.get("quartz_cron_expression")
            if "0/2 * * * ?" not in quartz_cron_expression:
                print(f"WARNING: Expected the schedule to be \"* 0/2 * * * ?\" but found \"{quartz_cron_expression}\".\n")
    
    print("All tests passed!")
    

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def start_job(self):
    job_name, notebook = self.get_job_config()
    job_id = self.client.jobs.get_by_name(job_name).get("job_id")
    run_id = self.client.jobs.run_now(job_id).get("run_id")
    print(f"Started job #{job_id}, run #{run_id}")

    self.client.runs.wait_for(run_id)

# COMMAND ----------

lesson_config.name = "cap_12"

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)

DA.conclude_setup()

