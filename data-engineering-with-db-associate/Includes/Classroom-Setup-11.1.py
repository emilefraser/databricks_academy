# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_sql(self, rows, sql):
    html = f"""<textarea style="width:100%" rows="{rows}"> \n{sql.strip()}</textarea>"""
    displayHTML(html)


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_users_table(self):
    self.print_sql(20, f"""
CREATE DATABASE IF NOT EXISTS {DA.schema_name}
LOCATION '{DA.paths.user_db}';

USE {DA.schema_name};

CREATE TABLE users (id INT, name STRING, value DOUBLE, state STRING);

INSERT INTO users
VALUES (1, "Yve", 1.0, "CA"),
       (2, "Omar", 2.5, "NY"),
       (3, "Elia", 3.3, "OH"),
       (4, "Rebecca", 4.7, "TX"),
       (5, "Ameena", 5.3, "CA"),
       (6, "Ling", 6.6, "NY"),
       (7, "Pedro", 7.1, "KY");

CREATE VIEW ny_users_vw
AS SELECT * FROM users WHERE state = 'NY';
""")
    

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_create_database_with_grants(self):
    self.print_sql(7, f"""
CREATE DATABASE {DA.schema_name}_derivative;

GRANT USAGE, READ_METADATA, CREATE, MODIFY, SELECT ON DATABASE `{DA.schema_name}_derivative` TO `users`;

SHOW GRANT ON DATABASE `{DA.schema_name}_derivative`;""")    
    

# COMMAND ----------

lesson_config.create_schema = False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

