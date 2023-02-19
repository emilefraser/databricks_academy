# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_dlt_policy(self):
    from dbacademy.dbhelper import ClustersHelper

    dlt_policy = DA.client.cluster_policies.get_by_name(ClustersHelper.POLICY_DLT_ONLY)
    if dlt_policy is None:
        dbgems.print_warning("WARNING: Policy Not Found", f"Could not find the cluster policy \"{ClustersHelper.POLICY_DLT_ONLY}\".\nPlease run the notebook Includes/Workspace-Setup to address this error.")
    
    return dlt_policy

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_pipeline_config(self):
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab"
    
    pipeline_name = f"DLT-Lab-82L-{DA.username}"
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
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{DA.paths.stream_path}" style="width:100%"></td>
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
    
    policy = self.get_dlt_policy()
    if policy is None: cluster = [{"num_workers": 0}]
    else:              cluster = [{"num_workers": 0, "policy_id": self.get_dlt_policy().get("policy_id")}]

    response = self.client.pipelines().create(
        name = pipeline_name, 
        storage = self.paths.storage_location, 
        target = self.schema_name, 
        notebooks = [path],
        development = not self.is_smoke_test(), # When testing, use production
        configuration = {
            "source": DA.paths.stream_path,
            "spark.master": "local[*]",
            "datasets_path": DA.paths.datasets,
        },
        clusters=cluster
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
    config_source = configuration.get("source")
    assert config_source == DA.paths.stream_path, f"Invalid source value. Expected \"{DA.paths.stream_path}\", found \"{config_source}\"."
    
    cluster_count = len(spec.get("clusters"))
    assert cluster_count == 1, f"Expected one, and only one, cluster configuration, found {cluster_count}. You can use the JSON UI to edit the configuration and remove the extra clusters."
    
    cluster = spec.get("clusters")[0]
    autoscale = cluster.get("autoscale")
    assert autoscale is None, f"Autoscaling should be disabled."
    
    num_workers = cluster.get("num_workers")
    assert num_workers == 0, f"Expected the number of workers to be 0, found {num_workers}."

    policy_id = cluster.get("policy_id")
    if policy_id is None:
        dbgems.print_warning("WARNING: Policy Not Set", f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\".")
    else:
        policy_name = self.client.cluster_policies.get_by_id(policy_id).get("name")
        if policy_id != self.get_dlt_policy().get("policy_id"):
            dbgems.print_warning("WARNING: Incorrect Policy", f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\", found \"{policy_name}\".")

    development = spec.get("development")
    assert development != self.is_smoke_test(), f"The pipline mode should be set to \"Development\"."
    
    channel = spec.get("channel")
    assert channel is None or channel == "CURRENT", f"Expected the channel to be Current but found {channel}."
    
    photon = spec.get("photon")
    assert photon == True, f"Expected Photon to be enabled."
    
    continuous = spec.get("continuous")
    assert continuous == False, f"Expected the Pipeline mode to be \"Triggered\", found \"Continuous\"."

    print("All tests passed!")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def start_pipeline(self):
    "Provided by DBAcademy, this function starts the pipline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    self.client = DBAcademyRestClient()

    pipeline_name, path = self.get_pipeline_config()

    pipeline = self.client.pipelines().get_by_name(pipeline_name)
    pipeline_id = pipeline.get("pipeline_id")
    
    # Start the pipeline
    start = self.client.pipelines().start_by_name(pipeline_name)
    update_id = start.get("update_id")

    # Get the status and block until it is done
    update = self.client.pipelines().get_update_by_id(pipeline_id, update_id)
    state = update.get("update").get("state")

    done = ["COMPLETED", "FAILED", "CANCELED"]
    while state not in done:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = self.client.pipelines().get_update_by_id(pipeline_id, update_id)
        state = update.get("update").get("state")
    
    print(f"The final state is {state}.")    
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

# COMMAND ----------

lesson_config.name = "dlt_lab_82"

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson() # First in a series
DA.init()

DA.paths.stream_path = f"{DA.paths.working_dir}/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.data_factory = DltDataFactory(DA.paths.stream_path)
DA.data_factory.load()

DA.conclude_setup()

