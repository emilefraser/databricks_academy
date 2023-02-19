# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_user_databases(self, drop_existing=False):
    configure_for = WorkspaceHelper.CONFIGURE_FOR_CURRENT_USER_ONLY if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(WorkspaceHelper.PARAM_CONFIGURE_FOR)
    self.workspace.databases.create_databases(configure_for=configure_for,
                                              drop_existing=drop_existing, 
                                              post_create=self.populate_database)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def drop_user_databases(self, drop_existing=False):
    configure_for = WorkspaceHelper.CONFIGURE_FOR_CURRENT_USER_ONLY if DBAcademyHelper.is_smoke_test() else dbgems.get_parameter(WorkspaceHelper.PARAM_CONFIGURE_FOR)
    self.workspace.databases.drop_databases(configure_for=configure_for)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def update_entitlements(self):
    group = self.client.scim.groups.get_by_name("users")
    self.client.scim.groups.add_entitlement(group.get("id"), "databricks-sql-access")
        

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def setup_completed(self):
    print(f"\nSetup completed in {int(time.time())-setup_start} seconds")

# COMMAND ----------

# The setup notebook will create databases and install datasets for us 
# so we don't need DBAcademyHelper.init() to do it for us here.
workspace_setup = dbgems.get_notebook_name() == "Workspace-Setup"
not_setup = not workspace_setup  # just for readability

lesson_config.name = None if workspace_setup else dbgems.clean_string(dbgems.get_notebook_name().split(" - ")[0])
lesson_config.create_schema = not_setup        # Only create schema if this is not setup
lesson_config.installing_datasets = not_setup  # Only instal the datasets if this is not setup

DA = DBAcademyHelper(course_config, lesson_config)

if workspace_setup or dbgems.is_generating_docs():
    # When running the setup notebooks or when generating docs we 
    # don't need to call these lesson-specific commands, just init()
    DA.init()
else:
    # In all other cases, configure as-if a lesson
    DA.reset_lesson()
    DA.init()
    DA.populate_database(DA.username, DA.schema_name, verbose=True)

DA.publisher = Publisher(DA)
DA.conclude_setup()

