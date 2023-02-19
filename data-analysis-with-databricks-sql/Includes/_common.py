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
from dbacademy.dbhelper import DBAcademyHelper, CourseConfig, LessonConfig

course_config = CourseConfig(course_code = "dawd",
                             course_name = "data-analysis-with-databricks-sql",
                             data_source_name = "data-analysis-with-databricks",
                             data_source_version = "v02",
                             install_min_time = "1 min",
                             install_max_time = "5 min",
                             remote_files = remote_files,
                             supported_dbrs = ["11.3.x-scala2.12", "11.3.x-photon-scala2.12", "11.3.x-cpu-ml-scala2.12"],
                             expected_dbrs = "11.3.x-scala2.12, 11.3.x-photon-scala2.12, 11.3.x-cpu-ml-scala2.12")

lesson_config = LessonConfig(name = None,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

# Start a timer so we can benchmark execution duration.
# This is only used by "Workspace-Setup"
setup_start = int(time.time())

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def clone_table(self, username, schema_name, table_name, location, verbose=False):
    start = dbgems.clock_start()
    existing_tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {schema_name}").collect()]
    
    if table_name in existing_tables: 
        if verbose: print(f"| skipping \"{table_name}\"...{clock_stopped(start)}")
        return
    elif verbose: print(f"| cloning \"{table_name}\"", end="...")
    
    physical_location = f"dbfs:/mnt/dbacademy-users/{username}/{self.course_config.course_name}/database.db/{table_name}"
    dbutils.fs.rm(physical_location, True)

    name = f"{schema_name}.{table_name}"
    table_location = f"{self.paths.datasets}/{location}"
    dbgems.sql(f"CREATE TABLE IF NOT EXISTS {name} SHALLOW CLONE delta.`{table_location}`;")
                      
    if verbose: print(dbgems.clock_stopped(start))

    return f"| created table \"{table_name}\"."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def populate_database(self, username, schema_name, verbose=False):
    if verbose: print("\nCloning tables:")
    
    msg = self.clone_table(username, schema_name, "flight_delays", "flights/departuredelays_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "sales", "retail-org/sales/sales_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "promo_prices", "retail-org/promo_prices/promo_prices_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "sales_orders", "retail-org/sales_orders/sales_orders_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "loyalty_segments", "retail-org/loyalty_segments/loyalty_segment_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "customers", "retail-org/customers/customers_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "suppliers", "retail-org/suppliers/suppliers_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "source_suppliers", "retail-org/suppliers/suppliers_delta", verbose=verbose)
    msg += "\n"+self.clone_table(username, schema_name, "gym_logs", "gym_logs/gym_logs_small_delta", verbose=verbose)
    
    if verbose: print()
    return msg

# COMMAND ----------

class Publisher:
    def __init__(self, da: DBAcademyHelper):
        import re

        self.da = da
        self.steps = list()
        self.width = 800

        self.lesson_name = dbgems.get_notebook_name()
        self.file_name = re.sub(r"[^a-zA-Z\d]", "-", self.lesson_name)+".html"
        while "--" in self.file_name: self.file_name = self.file_name.replace("--", "-")

        self.answer_is = """
            function answerIs(self, expected) {
                if (self.value === "") {
                    self.style.backgroundColor="#ffffff";
                } else if (self.value.toLowerCase().includes(expected)) {
                    self.style.backgroundColor="#7ffe78";
                } else {
                    self.style.backgroundColor="#ffb9bb";
                }
            }
        """

        self.common_style = f"""
            <style>
                body {{
                    background-color: #f9f7f4;
                    font-family: 'DM Sans', serif;
                }}
                h2 {{
                    color: #ff3621;
                }}
                h3 {{
                    margin-left: 15px
                }}
                ol {{
                    margin-left: -50px; 
                    font-family:sans-serif; 
                    color: #618794;
                }}
                td {{
                    padding: 5px;
                    border-bottom: 1px solid #ededed;
                }}
                tr.selected td {{
                    color: white;
                    background-color: red;
                }}
                .monofont {{font-family: monospace; font-size: 14px}}
                .content {{max-width: 800px; margin: auto; padding-left: 50px}}
                .image-icon-inline {{display:inline; vertical-align: middle; margin: 0 10px 0 10px}}
                .instructions-div {{padding-left: 40px}}
            </style>""".strip()
    
    def add_step(self, include_use, *, instructions, statements=None):
        return self.add(include_use=include_use,
                        instructions=instructions,
                        statements=statements,
                        test_code=None,
                        label=None,
                        expected=None,
                        length=-1)

    def add_validation(self, include_use, *, instructions, statements, test_code, label, expected, length):
        return self.add(include_use=include_use,
                        instructions=instructions,
                        statements=statements,
                        test_code=test_code,
                        label=label,
                        expected=expected,
                        length=length)

    def add(self, *, include_use, instructions, statements, test_code, label, expected, length):
        # Convert single statement to the required list
        if statements is None: statements = []
        if type(statements) == str: statements = [statements]
        
        step = Step(publisher=self,
                    number=len(self.steps)+1,
                    instructions=instructions,
                    width=self.width,
                    test_code=test_code,
                    label=label,
                    expected=expected,
                    length=length)

        if include_use:
            step.add("USE {schema_name};")

        for statement in statements:
            step.add(statement)
            
        self.steps.append(step)
        return step
        
    def find_repo(self):

        for i in range(1, 10):
            repo_path = dbgems.get_notebook_dir(offset=-i)
            status = self.da.client.workspace().get_status(repo_path)
            if status.get("object_type") == "REPO":
                return status

        raise Exception(f"The repo directory was not found")

    def publish(self, include_inputs=True):
        import os

        if dbgems.get_cloud() != "AWS":
            message = f"Skipping publish while running on {dbgems.get_cloud()}"
            print(message)
            return f"<html><body><p>{message}</p></body></html>"
        
        html = self.to_html(self.lesson_name, include_inputs)

        repo_path = self.find_repo().get("path")
        target_file = f"/Workspace{repo_path}/docs/{self.file_name}"
        print(f"Target:  {target_file}")
        
        target_dir = "/".join(target_file.split("/")[:-1])
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        
        with open(target_file, "w") as w:
            w.write(html)
        
        url = f"https://urban-umbrella-8e9e1ba3.pages.github.io/{self.file_name}"
        dbgems.display_html(f"<a href='{url}' target='_blank'>GitHub Page</a> (Changes must be committed before seeing the new page)")

        return html
        
    def to_html(self, lesson_name, include_inputs):
        from datetime import date
        
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <title>{lesson_name}</title>
                <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
                {self.common_style}
            </head>
            <body onload="loaded()">
                <div class="content">
                    <img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/db-academy-rgb-1200px_no_bg.png" 
                        alt="Databricks Learning" 
                        style="width: 600px; margin-left: 100px; margin-right: 100px">
                <hr/>
                <h1>{lesson_name}</h1>
        
                <div id="inputs">
                    <p>The two fields below are used to customize queries used in this course. Enter your schema (database) name and username, and press "Enter" to populate necessary information in the queries on this page.</p>
                    <table>
                        <tr>
                            <td style="white-space:nowrap">Schema Name:&nbsp;</td>
                            <td><input id="schema_name" type="text" style="width:40em" onchange="update();"></td>
                        </tr><tr>
                            <td style="white-space:nowrap">Username:&nbsp;</td>
                            <td><input id="username" type="text" style="width:40em" onchange="update();"></td>
                        </tr>
                    </table>
                </div>"""
        
        for step in self.steps:
            html += "<hr/>\n"
            html += step.to_html()
        
        # The ID elements that must be updated at runtime
        ids = [s.id for s in self.steps]  
        
        # F-strings complicate how to compose the following block of code so we inject the
        # following tokens to skirt various escaping issues related to f-strings
        username_token = "{username}"     # The runtime token for the username
        schema_name_token = "{schema_name}"       # The runtime token for the schema_name
        
        # Create the Javascript used to update the HTMl at runtime
        html += f"""<script type="text/javascript">
            {self.answer_is}
            function loaded() {{
                let data = document.cookie;
                if (data != null && data.trim() !== "") {{
                    let parts = data.split(";");
                    for (let i = 0; i < parts.length; i++) {{
                        let key_value = parts[i].trim();
                        let key = key_value.split("=")[0].trim();
                        let value = key_value.split("=")[1].trim();
                        if (value !== "n/a") {{
                            if (key === "{self.da.course_config.course_code}_schema_name") {{
                                document.getElementById("schema_name").value = value;
                            }} else if (key === "{self.da.course_config.course_code}_username") {{
                                document.getElementById("username").value = value;
                            }} else {{
                                console.log("Unknown cookie: "+key);
                            }}
                        }}
                    }}
                }}
                update();
            }}
            function update() {{      
                let schema_name = document.getElementById("schema_name").value;
                let username = document.getElementById("username").value;
                let ids = {ids};

                if (schema_name === null || username === null || 
                    schema_name === "" || username === "") {{
                    for (let i = 0; i < ids.length; i++) {{
                        document.getElementById(ids[i]+"-test-ta").disabled = true;
                        document.getElementById(ids[i]+"-sql-ta").disabled = true;

                        document.getElementById(ids[i]+"-test-btn").disabled = true;
                        document.getElementById(ids[i]+"-sql-btn").disabled = true;

                        document.getElementById(ids[i]+"-test-ta").value = document.getElementById(ids[i]+"-test-backup").value
                        document.getElementById(ids[i]+"-sql-ta").value = document.getElementById(ids[i]+"-sql-backup").value
                    }}
                }} else if (schema_name == "n/a" || username === "n/a") {{
                    for (let i = 0; i < ids.length; i++) {{
                        document.getElementById(ids[i]+"-test-ta").disabled = false;
                        document.getElementById(ids[i]+"-sql-ta").disabled = false;

                        document.getElementById(ids[i]+"-test-btn").disabled = false;
                        document.getElementById(ids[i]+"-sql-btn").disabled = false;
                    }}
                }} else {{
                    let illegals = ["<",">","\\"","'","\\\\","/"]; 
                    for (let i= 0; i < illegals.length; i++) {{
                        if (schema_name.includes(illegals[i])) {{
                            alert("Please double check your schema (database) name.\\nIt cannot not include the " + illegals[i] + " symbol.");
                            return;
                        }}
                        if (username.includes(illegals[i])) {{
                            alert("Please double check your username.\\nIt cannot not include the " + illegals[i] + " symbol.");
                            return;
                        }}
                    }}
                    if (schema_name.includes("@")) {{
                        alert("Please double check your schema (database) name.\\nIt should not include the @ symbol.");
                        return;
                    }}
                    if (username.includes("@") === false) {{
                        alert("Please double check your username.\\nIt should include the @ symbol.");
                        return;
                    }}
                    for (let i = 0; i < ids.length; i++) {{
                        document.getElementById(ids[i]+"-test-ta").disabled = false;
                        document.getElementById(ids[i]+"-sql-ta").disabled = false;

                        document.getElementById(ids[i]+"-test-btn").disabled = false;
                        document.getElementById(ids[i]+"-sql-btn").disabled = false;

                        document.getElementById(ids[i]+"-test-ta").value = document.getElementById(ids[i]+"-test-backup").value
                                                                                   .replaceAll("{schema_name_token}", schema_name)
                                                                                   .replaceAll("{username_token}", username);

                        document.getElementById(ids[i]+"-sql-ta").value = document.getElementById(ids[i]+"-sql-backup").value
                                                                                  .replaceAll("{schema_name_token}", schema_name)
                                                                                  .replaceAll("{username_token}", username);

                        document.cookie = "{self.da.course_config.course_code}_schema_name="+schema_name;
                        document.cookie = "{self.da.course_config.course_code}_username="+username;
                    }}
                }}
            }}
        </script>"""
        
        if not include_inputs:
            html += f"""<script type="text/javascript">
                    document.getElementById("schema_name").value = "n/a";
                    document.getElementById("username").value = "n/a";
                    document.getElementById("inputs").style.display = "none";
            </script>"""
        
        version = dbgems.get_parameter("version", "N/A")
        
        html += f"""
        <hr/>
        <div>
            <div>&copy; {date.today().year} Databricks, Inc. All rights reserved.</div>
            <div>Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.</div>
            <div style="margin-top:1em">
                <div style="float:left">
                    <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
                    <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
                </div>
                <div style="float:right">{version}</div>
            </div>               
        </div>"""
            
        html += """</div></body></html>"""
        return html

# COMMAND ----------

class Step:
    def __init__(self, publisher: Publisher, number: int, instructions: str, width: int, test_code: str = None, label: str = None, expected: str = None, length: int = -1):

        assert type(publisher) == Publisher, f"Expected the parameter \"da\" to be of type DBAcademyHelper, found {type(publisher)}"
        assert type(number) == int, f"Expected the parameter \"number\" to be of type int, found {type(number)}"
        assert type(width) == int, f"Expected the parameter \"width\" to be of type int, found {type(width)}"
        assert instructions is None or type(instructions) == str, f"Expected the parameter \"instructions\" to be of type str, found {type(instructions)}"
        assert test_code is None or type(test_code) == str, f"Expected the parameter \"test_code\" to be of type str, found {type(test_code)}"
        assert label is None or type(label) == str, f"Expected the parameter \"label\" to be of type str, found {type(label)}"
        assert expected is None or type(expected) == str, f"Expected the parameter \"expected\" to be of type str, found {type(expected)}"
        assert type(length) == int, f"Expected the parameter \"length\" to be of type int, found {type(length)}"

        self.publisher = publisher
        self.da = publisher.da
        self.width = width
        
        self.number = number
        self.id = f"step-{number}"
        self.instructions = instructions
        
        self.statements = []

        self.test_code = test_code.strip() if test_code is not None else ""
        self.label = label
        self.expected = expected
        self.length = length

    def add(self, statement):
        assert statement is not None, "The statement must be specified"
        statement = statement.strip()
        if statement != "":
            assert statement.endswith(";"), f"Expected statement to end with a semi-colon:\n{statement}"
            assert statement.count(";") == 1, f"Found more than one statement in the specified SQL string:\n{statement}"
        self.statements.append(statement)
    
    def update_statement(self, statement):
        statement = statement.replace("{schema_name}", self.da.schema_name)
        statement = statement.replace("{username}", self.da.username)

        self.validate_statement(statement)
        return statement.strip()

    @staticmethod
    def validate_statement(statement):
        import re

        test = re.search(r"{{\s*[\w.]*\s*}}", statement)
        if test is not None:
            raise Exception(f"Found existing mustache template: {test}")

        test = re.search(r"{\s*[\w.]*\s*}", statement)
        if test is not None:
            raise Exception(f"Found existing mustache template: {test}")

    def render(self, debug=False):
        if dbgems.is_generating_docs():
            print(f"Rendering suppressed; publishing")
            return None
        else:
            html = self.to_html(render_alone=True, debug=debug)
            dbgems.display_html(html)
            return html
        
    def execute(self):
        if dbgems.is_generating_docs():
            print(f"Execution suppressed; publishing")
        else:
            for statement in self.statements:
                if len(statement) == 0:
                    print(f"")
                else:
                    statement = self.update_statement(statement)
                    print(f"- Executing " + ("-"*68))
                    print(statement)
                    print("-"*80)
                    results = dbgems.sql(statement)
                    dbgems.display(results)

    def to_html(self, render_alone=False, debug=False):
        sql = ""
        for i, statement in enumerate(self.statements):
            sql += statement.strip()
            sql += "\n"
        sql = sql.strip()
        sql_row_count = len(sql.split("\n"))+1

        html = ""

        if render_alone:
            # This is a partial render, so we need body and style tags
            html = f"""
                <!DOCTYPE html>
                <html lang="en">
                    <head>
                        <meta charset="utf-8"/>
                        <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
                        {self.publisher.common_style}
                        <script type="text/javascript">
                            {self.publisher.answer_is}
                        </script>
                    </head>
                    <body>
                        <div style="border-style: outset; padding: 5px; width:{self.width+5}">"""

        html += f"""<div id="{self.id}-wrapper" style="width:{self.width}px;">"""

        if self.instructions is not None:
            instructions = self.instructions
            if "{step}" in instructions: instructions = instructions.format(step=self.number)

            html += f"""<div id="{self.id}-instruction" style="margin-bottom:1em">{instructions}</div>"""

        # This is the validation logic
        display_test_code = "display: none;" if self.test_code == "" else ""
        display_button = "display:none;" if sql == "" else ""
        test_row_count = len(self.test_code.split("\n")) + 1
        html += f"""
        <div style="{display_test_code}">
            <div>
                <textarea id="{self.id}-test-ta" style="width:100%; padding:10px; overflow-x:auto" rows="{test_row_count}">{self.test_code}</textarea>
                <textarea id="{self.id}-test-backup" style="display:none;">{self.test_code}</textarea>
            </div>
            <div style="text-align:right; {display_test_code}; margin-top:5px">
                <button id="{self.id}-test-show-btn" type="button" style="{display_button}" 
                        onclick="document.getElementById('{self.id}-sql').style.display = 'block';">Show Answer</button>
                <button id="{self.id}-test-btn" type="button"  onclick="
                    let ta = document.getElementById('{self.id}-test-ta');
                    ta.select();
                    ta.setSelectionRange(0, ta.value.length);
                    navigator.clipboard.writeText(ta.value);">Copy</button>
            </div>
        </div>  
          """
        if self.label is not None:
            html+= f"""
        <div>
            <table style="margin:1em 0; border-collapse:collapse; width:{self.width}px;">
                <tbody>
                    <tr>
                        <td style="background-color: #D1E2FF; width: 100%; text-align:left;">{self.label}</td>
                        <td style="background-color: #D1E2FF; width: 1em; text-align:right;">
                            <input type="text" size="{self.length}" onchange="answerIs(this, ['{self.expected}']);" style="background-color: rgb(255, 255, 255);">
                        </td>
                    </tr>    
                </tbody>
            </table>
        </div>
        """

        # This is the "standard" display for SQL content
        if display_test_code == "" or sql == "":
            # We are showing test code so don't display SQL or there is no SQL
            display_sql = "display: none;"
        else:
            display_sql = ""

        html += f"""
        <div id="{self.id}-sql" style="width:{self.width}px; {display_sql}">
            <div>
                <textarea id="{self.id}-sql-ta" style="width:100%; padding:10px" rows="{sql_row_count}">{sql}</textarea>
                <textarea id="{self.id}-sql-backup" style="display:none;">{sql}</textarea>
            </div>
            <div style="text-align:right; margin-top:5px">
                <button id="{self.id}-sql-btn" type="button"  onclick="
                    let ta = document.getElementById('{self.id}-sql-ta');
                    ta.select();
                    ta.setSelectionRange(0, ta.value.length);
                    navigator.clipboard.writeText(ta.value);">Copy</button>
            </div>
        </div>
        """

        html += "</div>"

        if render_alone:
            # If the style was specified,
            # we need to provide closing tags
            html += "</div></body></html>"

        if debug:
            file_name = self.publisher.file_name.replace(".html", f"_{self.number+1}.html")
            dbgems.get_dbutils().fs.put(f"dbfs:/FileStore/{file_name}", contents=html, overwrite=True)
            dbgems.display_html(f"""<a href="/files/{file_name}">download</a>""")

        return html

