# Databricks notebook source
def __validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert response.status_code == 200, "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError("{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e

def __install_libraries():
    global pip_command
    
    specified_version = f"v3.0.23"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print("** Dependency Version Overridden *******************************************************************")
        print(f"* This course was built for {specified_version} of the DBAcademy Library, but it is being overridden via the Spark")
        print(f"* configuration variable \"{key}\". The use of version v3.0.23 is not advised as we")
        print(f"* cannot guarantee compatibility with this version of the course.")
        print("****************************************************************************************************")

    try:
        from dbacademy import dbgems  
        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
        else:
            print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}.")
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
        else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}")
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()

__install_libraries()

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

import time
from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "dewd",
                             course_name = "data-engineering-with-databricks",
                             data_source_name = "data-engineering-with-databricks",
                             data_source_version = "v02",
                             install_min_time = "5 min",
                             install_max_time = "15 min",
                             remote_files = remote_files,
                             supported_dbrs = ["11.3.x-scala2.12", "11.3.x-photon-scala2.12", "11.3.x-cpu-ml-scala2.12"],
                             expected_dbrs = "11.3.x-scala2.12, 11.3.x-photon-scala2.12, 11.3.x-cpu-ml-scala2.12")

# For this course, these values will be true 99% of the time.
lesson_config = LessonConfig(name = None,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = True,
                             enable_ml_support = False)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_cluster_params(self, params: dict, task_indexes: list):

    if not self.is_smoke_test():
        return params
    
    for task_index in task_indexes:
        # Need to modify the parameters to run run as a smoke-test.
        task = params.get("tasks")[task_index]
        del task["existing_cluster_id"]

        cluster_params =         {
            "num_workers": "0",
            "spark_version": self.client.clusters().get_current_spark_version(),
            "spark_conf": {
              "spark.master": "local[*]"
            },
        }

        instance_pool_id = self.client.clusters().get_current_instance_pool_id()
        if instance_pool_id is not None: cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:                            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        task["new_cluster"] = cluster_params
        
    return params


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def clone_source_table(self, table_name, source_path, source_name=None):
    import time
    start = int(time.time())

    source_name = table_name if source_name is None else source_name
    print(f"Cloning the {table_name} table from {source_path}/{source_name}", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)

    total = spark.read.table(table_name).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

class DltDataFactory:
    def __init__(self, stream_path):
        self.stream_path = stream_path
        self.source = f"{DA.paths.datasets}/healthcare/tracker/streaming"
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.stream_path)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.stream_path}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.stream_path}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

