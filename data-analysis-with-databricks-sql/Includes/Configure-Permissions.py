# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Clone the course_config with one minor change.
course_config = CourseConfig(course_code = course_config.course_code,
                             course_name = course_config.course_name,
                             data_source_name = course_config.data_source_name,
                             data_source_version = course_config.data_source_version,
                             install_min_time = course_config.install_min_time,
                             install_max_time = course_config.install_max_time,
                             remote_files = course_config.remote_files,
                             supported_dbrs = ["10.4.x-scala2.12"],
                             expected_dbrs = "10.4.x-scala2.12")

lesson_config.create_schema = False
lesson_config.installing_datasets = False

DA = DBAcademyHelper(course_config, lesson_config)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_user_grants(self, username: str):
    schema_name = self.to_schema_name(username=username, course_code=self.course_config.course_code, lesson_name=None)
    
    if spark.sql("SHOW DATABASES").filter(f"databaseName = '{schema_name}'").count() == 1:
        spark.sql(f"GRANT ALL PRIVILEGES ON DATABASE `{schema_name}` TO `{username}`")
        spark.sql(f"GRANT ALL PRIVILEGES ON ANY FILE TO `{username}`")
        spark.sql(f"ALTER DATABASE {schema_name} OWNER TO `{username}`")

# COMMAND ----------

from dbacademy.dbhelper.workspace_helper_class import WorkspaceHelper

configure_for = WorkspaceHelper.CONFIGURE_FOR_CURRENT_USER_ONLY if DBAcademyHelper.is_smoke_test() else WorkspaceHelper.CONFIGURE_FOR_ALL_USERS
print(f"Configuring for \"{configure_for}\"")

usernames = DA.workspace.get_usernames(configure_for)
WorkspaceHelper.do_for_all_users(usernames, DA.update_user_grants)

