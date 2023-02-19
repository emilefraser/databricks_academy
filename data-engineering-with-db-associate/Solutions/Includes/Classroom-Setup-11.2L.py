# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.name = "acls_lab"
lesson_config.create_schema=False

DA = DBAcademyHelper(course_config, lesson_config)
DA.reset_lesson()
DA.init()
DA.conclude_setup()

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_sql(self, rows, sql):
    displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_query(self):
    import re
    import random

    self.print_sql(23, f"""
CREATE DATABASE IF NOT EXISTS {DA.schema_name}
LOCATION '{DA.paths.user_db}';

USE {DA.schema_name};
    
CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN); 

INSERT INTO beans
VALUES ('black', 'black', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('lentils', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('jelly', 'rainbow', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('pinto', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('green', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('beanbag chair', 'white', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('lentils', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('kidney', 'red', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
       ('castor', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])});

CREATE VIEW tasty_beans
AS SELECT * FROM beans WHERE delicious = true;
    """)


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_confirmation_query(self, username):
    import re
    # clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    database = DA.schema_name #.replace(DA.clean_username, clean_username)
    
    self.print_sql(11, f"""
USE {database};

SELECT * FROM beans;
SELECT * FROM tasty_beans;
SELECT * FROM beans MINUS SELECT * FROM tasty_beans;

UPDATE beans
SET color = 'pink'
WHERE name = 'black'
""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_union_query(self):
    self.print_sql(6, f"""
USE {DA.schema_name};

SELECT * FROM beans
UNION ALL TABLE beans;""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_derivative_view(self):
    self.print_sql(7, f"""
USE {DA.schema_name};

CREATE VIEW our_beans 
AS SELECT * FROM beans
UNION ALL TABLE beans;
""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_their_db(self, their_username):
    return self.to_schema_name(username=their_username, 
                               course_code=self.course_config.course_code,
                               lesson_name=self.lesson_config.name)

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_partner_view(self, their_username):
    self.print_sql(7, f"""
USE {self.get_their_db(their_username)};

SELECT name, color, delicious, sum(grams)
FROM our_beans
GROUP BY name, color, delicious;""")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def generate_delete_query(self, their_username):
    
    self.print_sql(5, f"""
USE {self.get_their_db(their_username)};

DELETE FROM beans
    """)


