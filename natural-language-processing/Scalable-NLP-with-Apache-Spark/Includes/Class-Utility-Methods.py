# Databricks notebook source
# Class Utility Methods

#############################################
# TAG API FUNCTIONS
#############################################

# Get all tags
def getTags() -> dict:
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags()
    )


# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
    values = getTags()[tagName]
    try:
        if len(values) > 0:
            return values
    except:
        return defaultValue


#############################################
# Get Databricks runtime major and minor versions
#############################################


def getDbrMajorAndMinorVersions() -> (int, int):
    import os

    dbrVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
    dbrVersion = dbrVersion.split(".")
    return (int(dbrVersion[0]), int(dbrVersion[1]))

#############################################
# USER, USERNAME, AND USERHOME FUNCTIONS
#############################################

# Get the user's username
def getUsername() -> str:
    import uuid

    try:
        return dbutils.widgets.get("databricksUsername")
    except:
        return getTag("user", str(uuid.uuid1()).replace("-", ""))


# Get the user's userhome
def getUserhome() -> str:
    username = getUsername()
    return "dbfs:/user/{}".format(username)


def getModuleName() -> str:
    # This will/should fail if module-name is not defined in the Classroom-Setup notebook
    return spark.conf.get("com.databricks.training.module-name")


def getLessonName() -> str:
    # If not specified, use the notebook's name.
    return (
        dbutils.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .getOrElse(None)
        .split("/")[-1]
    )


def getWorkingDir(courseType: str) -> str:
    import re

    langType = "p"  # for python
    moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName()).lower()
    lessonName = re.sub(r"[^a-zA-Z0-9]", "_", getLessonName()).lower()
    workingDir = "{}/{}/{}_{}{}".format(
        getUserhome(), moduleName, lessonName, langType, courseType
    )
    return (
        workingDir.replace("__", "_")
        .replace("__", "_")
        .replace("__", "_")
        .replace("__", "_")
    )


#############################################
# VERSION ASSERTION FUNCTIONS
#############################################

# When migrating DBR versions this should be one
# of the only two places that needs to be updated
latestDbrMajor = 8
latestDbrMinor = 4

# Assert an appropriate Databricks Runtime version
# def assertDbrVersion(
#     expected: str,
#     latestMajor: int = latestDbrMajor,
#     latestMinor: int = latestDbrMinor,
#     display: bool = True,
# ):

#     expMajor = latestMajor
#     expMinor = latestMinor

#     if expected and expected != FILL_IN:
#         expMajor = int(expected.split(".")[0])
#         expMinor = int(expected.split(".")[1])

#     (major, minor) = getDbrMajorAndMinorVersions()

#     if (major < expMajor) or (major == expMajor and minor < expMinor):
#         msg = f"This notebook must be run on DBR {expMajor}.{expMinor} or newer. Your cluster is using {major}.{minor}. You must update your cluster configuration before proceeding."

#         raise AssertionError(msg)

#     if major != expMajor or minor != expMinor:
#         html = f"""
#       <div style="color:red; font-weight:bold">WARNING: This notebook was tested on DBR {expMajor}.{expMinor}, but we found DBR {major}.{minor}.</div>
#       <div style="font-weight:bold">Using an untested DBR may yield unexpected results and/or various errors</div>
#       <div style="font-weight:bold">Please update your cluster configuration and/or <a href="https://academy.databricks.com/" target="_blank">download a newer version of this course</a> before proceeding.</div>
#     """

#     else:
#         html = f"Running on <b>DBR {major}.{minor}</b>"

#     if display:
#         displayHTML(html)
#     else:
#         print(html)

#     return f"{major}.{minor}"


############################################
# USER DATABASE FUNCTIONS
############################################


def getDatabaseName(
    courseType: str, username: str, moduleName: str, lessonName: str
) -> str:
    import re

    langType = "p"  # for python
    databaseName = (
        username + "_" + moduleName + "_" + lessonName + "_" + langType + courseType
    )
    databaseName = databaseName.lower()
    databaseName = re.sub("[^a-zA-Z0-9]", "_", databaseName)
    return (
        databaseName.replace("__", "_")
        .replace("__", "_")
        .replace("__", "_")
        .replace("__", "_")
    )


# Create a user-specific database
def createUserDatabase(
    courseType: str, username: str, moduleName: str, lessonName: str
) -> str:
    databaseName = getDatabaseName(courseType, username, moduleName, lessonName)

    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
    spark.sql("USE {}".format(databaseName))

    return databaseName


#############################################
# Legacy Testing Functions
#############################################

# Test results dict to store results
testResults = dict()

# Hash a string value
def toHash(value):
    from pyspark.sql.functions import hash
    from pyspark.sql.functions import abs

    values = [(value,)]
    return (
        spark.createDataFrame(values, ["value"])
        .select(abs(hash("value")).cast("int"))
        .first()[0]
    )


# Clear the testResults map
def clearYourResults(passedOnly=True):
    whats = list(testResults.keys())
    for what in whats:
        passed = testResults[what][0]
        if passed or passedOnly == False:
            del testResults[what]


# Validate DataFrame schema
def validateYourSchema(what, df, expColumnName, expColumnType=None):
    label = "{}:{}".format(expColumnName, expColumnType)
    key = "{} contains {}".format(what, label)

    try:
        actualType = df.schema[expColumnName].dataType.typeName()

        if expColumnType == None:
            testResults[key] = (True, "validated")
            print("""{}: validated""".format(key))
        elif actualType == expColumnType:
            testResults[key] = (True, "validated")
            print("""{}: validated""".format(key))
        else:
            answerStr = "{}:{}".format(expColumnName, actualType)
            testResults[key] = (False, answerStr)
            print("""{}: NOT matching ({})""".format(key, answerStr))
    except:
        testResults[what] = (False, "-not found-")
        print("{}: NOT found".format(key))


# Validate an answer
def validateYourAnswer(what, expectedHash, answer):
    # Convert the value to string, remove new lines and carriage returns and then escape quotes
    if answer == None:
        answerStr = "null"
    elif answer is True:
        answerStr = "true"
    elif answer is False:
        answerStr = "false"
    else:
        answerStr = str(answer)

    hashValue = toHash(answerStr)

    if hashValue == expectedHash:
        testResults[what] = (True, answerStr)
        print("""{} was correct, your answer: {}""".format(what, answerStr))
    else:
        testResults[what] = (False, answerStr)
        print("""{} was NOT correct, your answer: {}""".format(what, answerStr))


# Summarize results in the testResults dict
def summarizeYourResults():
    html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""

    whats = list(testResults.keys())
    whats.sort()
    for what in whats:
        passed = testResults[what][0]
        answer = testResults[what][1]
        color = "green" if (passed) else "red"
        passFail = "passed" if (passed) else "FAILED"
        html += """<tr style='font-size:larger; white-space:pre'>
                  <td>{}:&nbsp;&nbsp;</td>
                  <td style="color:{}; text-align:center; font-weight:bold">{}</td>
                  <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;{}</td>
                </tr>""".format(
            what, color, passFail, answer
        )
    html += "</table></body></html>"
    displayHTML(html)


# Log test results to a file
def logYourTest(path, name, value):
    value = float(value)
    if '"' in path:
        raise AssertionError("The name cannot contain quotes.")

    dbutils.fs.mkdirs(path)

    csv = """ "{}","{}" """.format(name, value).strip()
    file = "{}/{}.csv".format(path, name).replace(" ", "-").lower()
    dbutils.fs.put(file, csv, True)


# Load test results from log file
def loadYourTestResults(path):
    from pyspark.sql.functions import col

    return spark.read.schema("name string, value double").csv(path)


# Load test results from log file into a dict
def loadYourTestMap(path):
    rows = loadYourTestResults(path).collect()

    map = dict()
    for row in rows:
        map[row["name"]] = row["value"]

    return map


# ****************************************************************************
# Utility method to determine whether a path exists
# ****************************************************************************


def pathExists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False


# ****************************************************************************
# Facility for advertising functions, variables and databases to the student
# ****************************************************************************
def allDone(advertisements):

    functions = dict()
    variables = dict()
    databases = dict()

    for key in advertisements:
        if (
            advertisements[key][0] == "f"
            and spark.conf.get(f"com.databricks.training.suppress.{key}", None)
            != "true"
        ):
            functions[key] = advertisements[key]

    for key in advertisements:
        if (
            advertisements[key][0] == "v"
            and spark.conf.get(f"com.databricks.training.suppress.{key}", None)
            != "true"
        ):
            variables[key] = advertisements[key]

    for key in advertisements:
        if (
            advertisements[key][0] == "d"
            and spark.conf.get(f"com.databricks.training.suppress.{key}", None)
            != "true"
        ):
            databases[key] = advertisements[key]

    html = ""
    if len(functions) > 0:
        html += "The following functions were defined for you:<ul style='margin-top:0'>"
        for key in functions:
            value = functions[key]
            html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        <span style="color: green; font-weight:bold">{key}</span>
        <span style="font-weight:bold">(</span>
        <span style="color: green; font-weight:bold; font-style:italic">{value[1]}</span>
        <span style="font-weight:bold">)</span>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
        html += "</ul>"

    if len(variables) > 0:
        html += "The following variables were defined for you:<ul style='margin-top:0'>"
        for key in variables:
            value = variables[key]
            html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        <span style="color: green; font-weight:bold">{key}</span>: <span style="font-style:italic; font-weight:bold">{value[1]} </span>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
        html += "</ul>"

    if len(databases) > 0:
        html += "The following database were created for you:<ul style='margin-top:0'>"
        for key in databases:
            value = databases[key]
            html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        Now using the database identified by <span style="color: green; font-weight:bold">{key}</span>: 
        <div style="font-style:italic; font-weight:bold">{value[1]}</div>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
        html += "</ul>"

    html += "All done!"
    displayHTML(html)


# ****************************************************************************
# Placeholder variables for coding challenge type specification
# ****************************************************************************
class FILL_IN:
    from pyspark.sql.types import Row, StructType

    VALUE = None
    LIST = []
    SCHEMA = StructType([])
    ROW = Row()
    INT = 0
    DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))


displayHTML("Defining courseware-specific utility methods...")

