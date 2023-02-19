# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import json, time
from dbacademy import dbgems
from dbacademy.dbrest.sql.endpoints import *
from dbacademy.dbrest import DBAcademyRestClient

# COMMAND ----------

client = DBAcademyRestClient()

naming_template="da-{da_name}@{da_hash}-{course}"

courses_map = {
    "Advanced Data Engineering with Databricks": {
        "code": "adewd",
        "name": "Advanced Data Engineering with Databricks",
        "dataset-repo": "advanced-data-engineering-with-databricks",
        "source-repo": "advanced-data-engineering-with-databricks-source",
    },
    "Advanced Machine Learning with Databricks": {
        "code": "amlwd",
        "name": "Advanced Machine Learning with Databricks",
        "dataset-repo": "advanced-machine-learning-with-databricks",
        "source-repo": "advanced-machine-learning-with-databricks-source",
    },
    "Apache Spark Programming with Databricks": {
        "code": "aspwd",
        "name": "Apache Spark Programming with Databricks",
        "dataset-repo": "apache-spark-programming-with-databricks",
        "source-repo": "apache-spark-programming-with-databricks-source",
    },
    "Data Analysis with Databricks": {
        "code": "dawd",
        "name": "Data Analysis with Databricks",
        "dataset-repo": "data-analysis-with-databricks",
        "source-repo": "data-analysis-with-databricks-source",
    },
    "Data Engineering with Databricks": {
        "code": "dewd",
        "name": "Data Engineering with Databricks",
        "dataset-repo": "data-engineering-with-databricks",
        "source-repo": "data-engineering-with-databricks-source",
    },
    "Deep Learning with Databricks": {
        "code": "dlwd",
        "name": "Deep Learning with Databricks",
        "dataset-repo": "deep-learning-with-databricks",
        "source-repo": "deep-learning-with-databricks-source",
    },
    "Just Enough Python for Spark": {
        "code": "jepfs",
        "name": "Just Enough Python For Spark",
        "dataset-repo": "just-enough-python-for-spark",
        "source-repo": "just-enough-python-for-spark-source",
    },
    "ML in Production": {
        "code": "mlip",
        "name": "ML in Production",
        "dataset-repo": "ml-in-production",
        "source-repo": "ml-in-production-source",
    },
    "Scalable Machine Learning with Apache Spark": {
        "code": "smlwas",
        "name": "Scalable Machine Learning with Apache Spark",
        "dataset-repo": "scalable-machine-learning-with-apache-spark",
        "source-repo": "scalable-machine-learning-with-apache-spark-source",
    }
}

# COMMAND ----------

def to_db_name(course_code, username):
    import re
    
    naming_template="da-{da_name}@{da_hash}-{course}"
    naming_params={"course": course_code}
    
    if "{da_hash}" in naming_template:
        assert naming_params.get("course", None) is not None, "The template is employing da_hash which requires course to be specified in naming_params"
        course = naming_params["course"]
        da_hash = abs(hash(f"{username}-{course}")) % 10000
        naming_params["da_hash"] = da_hash

    naming_params["da_name"] = username.split("@")[0]
    db_name = naming_template.format(**naming_params)    
    return re.sub("[^a-zA-Z0-9]", "_", db_name)

# COMMAND ----------

def init_course():
    all_courses = [""]
    all_courses.extend(courses_map.keys())
    all_courses.sort()
    dbutils.widgets.combobox("course", "", all_courses, "Course")

def init_usernames():
    all_usernames = [r.get("userName") for r in client.scim().users().list()]
    all_usernames.sort()
    all_usernames.insert(0, "All Users")
    dbutils.widgets.multiselect("usernames", "All Users", all_usernames, "Users")    

# COMMAND ----------

def get_course():
    course = dbutils.widgets.get("course")
    assert len(course) > 0, "Please select a course before proceeding."
    return course        

# COMMAND ----------

def get_usernames():
    selected_usernames = dbutils.widgets.get("usernames").split(",")
    
    users = client.scim().users().list() if "All Users" in selected_usernames else client.scim().users().to_users_list(selected_usernames)
    usernames = [u.get("userName") for u in users]
    assert len(usernames) > 0, "Please select either \"All Users\" or a valid subset of users"
    return usernames

# COMMAND ----------

def get_course_name():
    course = get_course()
    return courses_map.get(course).get("name")

# COMMAND ----------

def get_course_code():
    course = get_course()
    return courses_map.get(course).get("code")

# COMMAND ----------

def get_source_repo():
    course = get_course()
    return courses_map.get(course).get("source-repo")

# COMMAND ----------

def get_dataset_repo():
    course = get_course()
    return courses_map.get(course).get("dataset-repo")

# COMMAND ----------

def get_data_source_base_uri():
    return f"wasbs://courseware@dbacademy.blob.core.windows.net/{get_dataset_repo()}"

def get_data_source_version():
    data_source_base_uri = get_data_source_base_uri()
    version_files = dbutils.fs.ls(data_source_base_uri)

    all_versions = [""]
    all_versions.extend([r.name[0:-1] for r in version_files])
    dbutils.widgets.dropdown("version", all_versions[-1], all_versions, "Version")
    data_source_version = dbutils.widgets.get("version")
    assert len(data_source_version) > 0, "Please select the dataset version"
    return data_source_version

def get_data_source_uri():
    data_source_base_uri = get_data_source_base_uri()
    data_source_uri = f"{data_source_base_uri}/{get_data_source_version()}"
    return data_source_uri


# COMMAND ----------

def get_naming_params():
    return {"course": get_course_code()}

