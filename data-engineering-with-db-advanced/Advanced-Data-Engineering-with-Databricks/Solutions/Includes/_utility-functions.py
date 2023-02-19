# Databricks notebook source
def _install_datasets(reinstall=False):
    import time

    min_time = "3 min"
    max_time = "10 min"

    data_source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/advanced-data-engineering-with-databricks/v01"
    print(f"The source for this dataset is\n{data_source_uri}/\n")

    print(f"Your dataset directory is\n{DA.hidden.datasets}\n")
    existing = DA.paths.exists(DA.hidden.datasets)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset.")
        print()
        validate_datasets()
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{DA.hidden.datasets}")
        dbutils.fs.rm(DA.hidden.datasets, True)

    print(f"""Installing the datasets to {DA.hidden.datasets}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    files = dbutils.fs.ls(data_source_uri)
    print(f"\nInstalling {len(files)+2} datasets: ")
    
    install_start = int(time.time())
    for f in files:
        start = int(time.time())
        print(f"Copying /{f.name[:-1]}", end="...")

        dbutils.fs.cp(f"{data_source_uri}/{f.name}", f"{DA.hidden.datasets}/{f.name}", True)
        print(f"({int(time.time())-start} seconds)")

    # TODO - move this into the storage so that we don't have to do it here
    start = int(time.time())
    print(f"Copying /user-lookup", end="...")
    (spark.read
          .format("json")
          .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
          .load(f"{DA.hidden.datasets}/user-reg")
          .selectExpr(f"sha2(concat(user_id,'BEANS'), 256) AS alt_id", "device_id", "mac_address", "user_id")
          .coalesce(1)
          .write
          .save(f"{DA.hidden.datasets}/user-lookup"))
    print(f"({int(time.time())-start} seconds)")

    # TODO - move this into the storage so that we don't have to do it here
    start = int(time.time())
    print(f"Copying /gym-mac-logs", end="...")
    (spark.read
          .schema("first_timestamp double, gym long, last_timestamp double, mac string")
          .json(f"{DA.hidden.datasets}/gym-logs")
          .coalesce(1)
          .write
          .save(f"{DA.hidden.datasets}/gym-mac-logs"))
    print(f"({int(time.time())-start} seconds)")
    
    print()
    validate_datasets()
    print(f"""\nThe install of the datasets completed successfully in {int(time.time())-install_start} seconds.""")  

DA.install_datasets = _install_datasets

# COMMAND ----------

def validate_path(expected, path):
    files = dbutils.fs.ls(path)
    message = f"Expected {expected} files, found {len(files)} in {path}"
    for file in files:
        message += f"\n{file.path}"
    
    if len(files) != expected:
      display(files)
      raise AssertionError(message)

def validate_datasets():  
    import time

    start = int(time.time())
    print("Validating datasets", end="...")

    validate_path(8, f"{DA.hidden.datasets}")
    validate_path(4, f"{DA.hidden.datasets}/bronze")
    validate_path(4, f"{DA.hidden.datasets}/bronze/topic=bpm")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=bpm/week_part=2019-48")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=bpm/week_part=2019-49")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=bpm/week_part=2019-50")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=bpm/week_part=2019-51")

    validate_path(20, f"{DA.hidden.datasets}/bronze/topic=user_info")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-16")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-19")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-20")
    validate_path(3, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-21")
    validate_path(3, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-22")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-23")
    validate_path(5, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-24")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-26")
    validate_path(5, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-27")
    validate_path(6, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-28")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-29")
    validate_path(3, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-30")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-31")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-32")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-33")
    validate_path(1, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-34")
    validate_path(5, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-48")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-49")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-50")
    validate_path(3, f"{DA.hidden.datasets}/bronze/topic=user_info/week_part=2019-51")

    validate_path(4, f"{DA.hidden.datasets}/bronze/topic=workout")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=workout/week_part=2019-48")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=workout/week_part=2019-49")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=workout/week_part=2019-50")
    validate_path(10, f"{DA.hidden.datasets}/bronze/topic=workout/week_part=2019-51")

    validate_path(9, f"{DA.hidden.datasets}/date-lookup")
    validate_path(5, f"{DA.hidden.datasets}/date-lookup/_delta_log")

    validate_path(85, f"{DA.hidden.datasets}/gym-logs")

    validate_path(11, f"{DA.hidden.datasets}/kafka-30min")
    validate_path(7, f"{DA.hidden.datasets}/kafka-30min/_delta_log")

    validate_path(2, f"{DA.hidden.datasets}/pii")
    validate_path(13, f"{DA.hidden.datasets}/pii/raw")
    validate_path(9, f"{DA.hidden.datasets}/pii/raw/_delta_log")
    validate_path(2, f"{DA.hidden.datasets}/pii/silver")
    validate_path(5, f"{DA.hidden.datasets}/pii/silver/_delta_log")

    validate_path(100, f"{DA.hidden.datasets}/user-reg")

    validate_path(2, f"{DA.hidden.datasets}/user-lookup")
    # validate_path(0, f"{DA.hidden.datasets}/user-lookup/_delta_log")

    validate_path(2, f"{DA.hidden.datasets}/gym-mac-logs")
    # validate_path(0, f"{DA.hidden.datasets}/gym-mac-logs/_delta_log")

    print(f"({int(time.time())-start} seconds)")   
    

# COMMAND ----------

def _block_until_stream_is_ready(query=None, name=None, min_batches=2):
    import time

    for q in spark.streams.active:
        query = q if q.name == name and name is not None else query

    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

DA.block_until_stream_is_ready = _block_until_stream_is_ready

# COMMAND ----------

class TaskConfig():
    def __init__(self, name, resource_type, resource, pipeline_id=None, depends_on=[], cluster="shared_cluster", params={}):
        self.name = name
        self.resource = resource
        self.pipeline_id = pipeline_id
        self.resource_type = resource_type
        self.depends_on = depends_on
        self.cluster = cluster
        self.params = params

class JobConfig():
    def __init__(self, job_name, tasks):
        self.job_name = job_name
        self.tasks = tasks

# COMMAND ----------

def _print_job_config():
    """
    Renders the configuration of the job as HTML
    """
    config = get_job_config()
    
    border_color = "1px solid rgba(0, 0, 0, 0.25)"
    td_style = f"white-space:nowrap; padding: 8px; border: 0; border-left: {border_color}; border-top: {border_color}"
    
    html = f"""  
    <p style="font-size: 16px">Job Name: <span style="font-weight:bold">{config.job_name}</span></p>
    
    <table style="width:100%; border-collapse: separate; border-spacing: 0; border-right: {border_color}; border-bottom: {border_color}; color: background-color: rgba(0, 0, 0, 0.8)">
        <tr>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:1em">Description</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:8em">Task Name</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1); width:11em">Task Type</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1)">Resource</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1)">Depends On</td>
            <td style="{td_style}; background-color: rgba(245,245,245,1)">Parameters</td>
        </tr>
    """
    for i, task in enumerate(config.tasks):
        html += f"""
            <tr>
                <td style="{td_style}">Task #{i+1}:</td>
                <td style="{td_style}"><input type="text" value="{task.name}" style="width:100%; font-weight: bold"></td>
                <td style="{td_style}; font-weight: bold">{task.resource_type}</td>
                <td style="{td_style}; font-weight: bold">{task.resource}</td>
                <td style="{td_style}; font-weight: bold">{", ".join(task.depends_on)}</td>
                <td style="{td_style}; font-weight: bold">{task.params}</td>
            </tr>"""
        
    html += "\n</table>"
    displayHTML(html)

DA.print_job_config = _print_job_config

# COMMAND ----------

def _create_job():
    """
    Creates the prescribed job.
    """
    import re, json
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    config = get_job_config()
    print(f"Creating the job {config.job_name}")

    # Delete the existing pipeline if it exists
    client.jobs().delete_by_name(config.job_name, success_only=False)

    course_name = re.sub("[^a-zA-Z0-9]", "-", DA.course_name)
    while "--" in course_name: course_name = course_name.replace("--", "-")
    
    params = {
        "name": f"{config.job_name}",
        "tags": {
            "dbacademy.course": course_name,
            "dbacademy.source": course_name
        },
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [],
        "job_clusters": [{
            "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "num_workers": 0,
                "spark_version": f"{client.clusters().get_current_spark_version()}",
                "spark_conf": { "spark.master": "local[*]" },
            },
        }]
    }
    
    for task in config.tasks:
        task_def = {
            "task_key": task.name,
        }
        params.get("tasks").append(task_def)
        if task.cluster is not None: task_def["job_cluster_key"] = task.cluster
        
        if task.pipeline_id is not None: 
            task_def["pipeline_task"] = {"pipeline_id": task.pipeline_id}
        else: 
            task_def["notebook_task"] = {
                "notebook_path": task.resource,
                "base_parameters": task.params
            }
            
        if len(task.depends_on) > 0:
            task_def["depends_on"] = list()
            for key in task.depends_on: task_def["depends_on"].append({"task_key":key})
        
    instance_pool_id = client.clusters().get_current_instance_pool_id()
    cluster = params.get("job_clusters")[0].get("new_cluster")
    if instance_pool_id:
        cluster["instance_pool_id"] = instance_pool_id
    else:
        node_type_id = client.clusters().get_current_node_type_id()
        cluster["node_type_id"] = node_type_id
        
    # print(json.dumps(params, indent=4))
    
    json_response = client.jobs().create(params)
    job_id = json_response["job_id"]
    print(f"Created job {job_id}")
    return job_id

DA.create_job = _create_job

# COMMAND ----------

def _start_job(job_id):
    "Starts the job and then blocks until it is TERMINATED or INTERNAL_ERROR"

    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    run_id = client.jobs().run_now(job_id).get("run_id")
    response = client.runs().wait_for(run_id)
    
    state = response.get("state").get("life_cycle_state")
    assert state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"], f"Expected final state: {state}"
    
DA.start_job = _start_job

# COMMAND ----------

def create_date_lookup():
    import time

    start = int(time.time())
    print(f"Creating date_lookup", end="...")

    spark.sql(f"""
      CREATE OR REPLACE TABLE date_lookup
      SHALLOW CLONE delta.`{DA.hidden.datasets}/date-lookup`
      LOCATION '{DA.paths.user_db}/date_lookup'
    """)

#     spark.sql(f"""
#       CREATE TABLE date_lookup
#       LOCATION '{DA.hidden.datasets}/date-lookup'
#     """)

    total = spark.read.table("date_lookup").count()
    assert total == 1096, f"Expected 1,096 records, found {total:,} in date_lookup"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
None # Suppressing Output

# COMMAND ----------

def _create_gym_mac_logs():
    import time
    start = int(time.time())
    print(f"Creating gym_mac_logs", end="...")

    spark.sql(f"""
      CREATE OR REPLACE TABLE gym_mac_logs
      SHALLOW CLONE delta.`{DA.hidden.datasets}/gym-mac-logs`
      LOCATION '{DA.paths.user_db}/gym_mac_logs'
    """)

#     spark.sql(f"""
#       CREATE TABLE gym_mac_logs
#       LOCATION '{DA.hidden.datasets}/gym-mac-logs'
#     """)

    total = spark.read.table("gym_mac_logs").count()
    assert total == 314, f"Expected 314 records, found {total:,} in gym_mac_logs"
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.create_gym_mac_logs = _create_gym_mac_logs

# COMMAND ----------

def _create_user_lookup():
    import time
    
    start = int(time.time())
    print(f"Creating user_lookup", end="...")

    spark.sql(f"""
      CREATE OR REPLACE TABLE user_lookup
      SHALLOW CLONE delta.`{DA.hidden.datasets}/user-lookup`
      LOCATION '{DA.paths.user_db}/user_lookup'
    """)

#     spark.sql(f"""
#       CREATE TABLE user_lookup
#       LOCATION '{DA.hidden.datasets}/user-lookup'
#     """)

    total = spark.sql(f"SELECT * FROM user_lookup").count()
    assert total == 100, f"Expected 100 records, found {total:,} in user_lookup"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")    
    
DA.create_user_lookup = _create_user_lookup

# COMMAND ----------

class DailyStreamingFactory:
    def __init__(self, target_dir, starting_batch=1, max_batch=16):
        self.target_dir = target_dir
        self.max_batch = max_batch
        self.batch = starting_batch
    
    def load_through(self, max_batch):
        import time
        
        start = int(time.time())
        print(f"Loading all batches from #{self.batch} through #{max_batch} to the daily stream", end="...")
        
        total = self.load_batch(self.batch, max_batch)
        self.batch = max_batch+1
        
        print(f"({int(time.time())-start} seconds, {total:,} records)")
    
    def load(self, continuous=False):
        import time
        
        start = int(time.time())
        
        if self.batch > self.max_batch:
            total = 0
            print("Data source exhausted", end="...")
            
        elif continuous == True:
            print("Loading all batches to the daily stream", end="...")
            total = self.load_batch(self.batch, self.max_batch)
            self.batch = self.max_batch+1
            
        else:
            print(f"Loading batch #{self.batch} to the daily stream", end="...")
            total = self.load_batch(self.batch, self.batch)
            self.batch += 1
            
        print(f"({int(time.time())-start} seconds, {total:,} records)")
    
    def load_batch(self, min_batch, max_batch):
        from pyspark.sql import functions as F

        # Refactoring didn't improve performance        
        # df = spark.read.load(f"{DA.hidden.datasets}/bronze")
        # if min_batch == 1 and max_batch == 1:
        #     # Day #1 and only day #1
        #     # Include all records from 12-01 and back
        #     df = df.filter("date <= '2019-12-01'")
        # elif min_batch == 1:
        #     # Day #1 through max_batch
        #     # Include all records from max_batch and back
        #     df = df.filter(f"date <= '2019-12-{max_batch:02d}'")
        # elif min_batch == max_batch:
        #     # Just one day out of the set, but not day #1
        #     df = df.filter(f"date = '2019-12-{max_batch:02d}'")
        # else:
        #     # Range of dates, but not day #1
        #     df = df.filter(f"date >= '2019-12-{min_batch:02d}' and date <= '2019-12-{max_batch:02d}'")

        # Crippling the predicate here. Experiments yeild no better result short
        # of restructuring the underlying datsets to support a date-based predicate
        df = (spark.read
                   .load(f"{DA.hidden.datasets}/bronze")
                   .withColumn("day", F.when(F.col("date") <= '2019-12-01', 1).otherwise(F.dayofmonth("date")))
                   .filter(F.col("day") >= min_batch)
                   .filter(F.col("day") <= max_batch)
                   .drop("date", "week_part", "day"))
              
        df.write.mode("append").format("json").save(self.target_dir)
        return df.count()

None # Suppressing Output

# COMMAND ----------

def init_source_daily():
    DA.paths.source_daily = f"{DA.paths.working_dir}/streams/daily"
    DA.daily_stream = DailyStreamingFactory(DA.paths.source_daily)
    
None # Suppressing Output

# COMMAND ----------

def _create_partitioned_bronze_table():
    import time
    
    start = int(time.time())
    print(f"Creating bronze", end="...")
    
    (spark.read
          .load(f"{DA.hidden.datasets}/bronze")
          .write
          .partitionBy("topic", "week_part")
          .option("path", f"{DA.paths.user_db}/bronze")
          .saveAsTable("bronze"))

    total = spark.read.table("bronze").count()
    assert total == 10841978, f"Expected 10,841,978 records, found {total:,} in bronze"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.create_partitioned_bronze_table = _create_partitioned_bronze_table

# COMMAND ----------

def _create_bronze_table():
    import time
    
    start = int(time.time())
    print(f"Creating bronze", end="...")
    
    spark.sql(f"""
      CREATE TABLE bronze
      SHALLOW CLONE delta.`{DA.hidden.datasets}/bronze`
      LOCATION '{DA.paths.user_db}/bronze'
    """) 
    
    total = spark.read.table("bronze").count()
    assert total == 10841978, f"Expected 10,841,978 records, found {total:,} in bronze"    
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.create_bronze_table = _create_bronze_table

# COMMAND ----------

# This is the solution from lesson 2.03 and is included
# here to fast-forward the student to this stage
def _process_bronze():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print(f"Processing the bronze table from the daily stream", end="...")
        
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    date_lookup_df = spark.table("date_lookup").select("date", "week_part")

    def execute_stream():
        query = (spark.readStream
                      .format("cloudFiles")
                      .schema(schema)
                      .option("cloudFiles.format", "json")
                      .load(DA.paths.source_daily)
                      .join(F.broadcast(date_lookup_df), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
                      .writeStream
                      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                      .partitionBy("topic", "week_part")
                      .option("path", f"{DA.paths.user_db}/bronze")
                      .trigger(availableNow=True)
                      .table("bronze"))
        query.awaitTermination()
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("bronze").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_bronze = _process_bronze

# COMMAND ----------

def _process_heart_rate_silver_v0():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the heart_rate_silver table", end="...")

    class Upsert:
        def __init__(self, sql_query, update_temp="stream_updates"):
            self.sql_query = sql_query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.sql_query)
    
    spark.sql("CREATE TABLE IF NOT EXISTS heart_rate_silver (device_id LONG, time TIMESTAMP, heartrate DOUBLE) USING DELTA")
    
    streamingMerge=Upsert("""
      MERGE INTO heart_rate_silver a
      USING stream_updates b
      ON a.device_id=b.device_id AND a.time=b.time
      WHEN NOT MATCHED THEN INSERT *
    """)

    def execute_stream():
        (spark.readStream
              .table("bronze")
              .filter("topic = 'bpm'")
              .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
              .select("v.*")
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["device_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate")
              .trigger(availableNow=True)
              .start()
              .awaitTermination())
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("heart_rate_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.process_heart_rate_silver_v0 = _process_heart_rate_silver_v0

None # Suppressing Output

# COMMAND ----------

def _process_heart_rate_silver():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the heart_rate_silver table", end="...")

    class Upsert:
        def __init__(self, sql_query, update_temp="stream_updates"):
            self.sql_query = sql_query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.sql_query)
    
    spark.sql("CREATE TABLE IF NOT EXISTS heart_rate_silver (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING) USING DELTA")
    
    streamingMerge=Upsert("""
      MERGE INTO heart_rate_silver a
      USING stream_updates b
      ON a.device_id=b.device_id AND a.time=b.time
      WHEN NOT MATCHED THEN INSERT *
    """)

    def execute_stream():
        (spark.readStream
              .table("bronze")
              .filter("topic = 'bpm'")
              .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
              .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["device_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate")
              .trigger(availableNow=True)
              .start()
              .awaitTermination())    
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
        
    total = spark.read.table("heart_rate_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.process_heart_rate_silver = _process_heart_rate_silver

None # Suppressing Output

# COMMAND ----------

def _process_workouts_silver():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the workouts_silver table", end="...")
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS workouts_silver (user_id INT, workout_id INT, time TIMESTAMP, action STRING, session_id INT) USING DELTA")
    
    class Upsert:
        def __init__(self, query, update_temp="stream_updates"):
            self.query = query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.query)
    
    sql_query = """
        MERGE INTO workouts_silver a
        USING workout_updates b
        ON a.user_id=b.user_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(sql_query, "workout_updates")
    
    def execute_stream():
        (spark.readStream
              .option("ignoreDeletes", True)
              .table("bronze")
              .filter("topic = 'workout'")
              .select(F.from_json(F.col("value").cast("string"), "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT").alias("v"))
              .select("v.*")
              .select("user_id", "workout_id", F.col("timestamp").cast("timestamp").alias("time"), "action", "session_id")
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["user_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/workouts")
              .queryName("workouts_silver")
              .trigger(availableNow=True)
              .start()
              .awaitTermination())
        
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("workouts_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_workouts_silver = _process_workouts_silver

None # Suppressing Output

# COMMAND ----------

def _process_completed_workouts():
    import time
    from pyspark.sql import functions as F

    start = int(time.time())
    print("Processing the completed_workouts table", end="...")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW TEMP_completed_workouts AS (
          SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
          FROM (
            SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
            FROM workouts_silver
            WHERE action = "start") a
          LEFT JOIN (
            SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
            FROM workouts_silver
            WHERE action = "stop") b
          ON a.user_id = b.user_id AND a.session_id = b.session_id
        )
    """)
    
    (spark.table("TEMP_completed_workouts").write.mode("overwrite").saveAsTable("completed_workouts"))
    
    total = spark.read.table("completed_workouts").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_completed_workouts = _process_completed_workouts

None # Suppressing Output

# COMMAND ----------

def _process_workout_bpm():
    import time
    from pyspark.sql.utils import AnalysisException
    
    start = int(time.time())
    print("Processing the workout_bpm table", end="...")

    spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")
    
    spark.sql("""
        SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
        FROM TEMP_heart_rate_silver c
        INNER JOIN (
          SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
          FROM completed_workouts a
          INNER JOIN user_lookup b
          ON a.user_id = b.user_id) d
        ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
        WHERE c.bpm_check = 'OK'""").createOrReplaceTempView("TEMP_workout_bpm")
    
    def execute_stream():
        (spark.table("TEMP_workout_bpm")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm")
            .option("path", f"{DA.paths.user_db}/workout_bpm")
            .trigger(availableNow=True)
            .table("workout_bpm")
            .awaitTermination())
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()

    total = spark.read.table("workout_bpm").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_workout_bpm = _process_workout_bpm

None # Suppressing Output    

# COMMAND ----------

def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank == 1")
        .drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

    (microBatchDF
        .filter("update_type = 'delete'")
        .select("alt_id", 
                F.col("updated").alias("requested"), 
                F.date_add("updated", 30).alias("deadline"), 
                F.lit("requested").alias("status"))
        .write
        .format("delta")
        .mode("append")
        .option("txnVersion", batchId)
        .option("txnAppId", "batch_rank_upsert")
        .option("path", f"{DA.paths.user_db}/delete_requests")
        .saveAsTable("delete_requests"))
    
def _process_users():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    
    start = int(time.time())
    print(f"Processing the users table", end="...")

    spark.sql(f"CREATE TABLE IF NOT EXISTS users (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP) USING DELTA")
    
    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
    >"""
    
    def execute_stream():
        (spark.readStream
            .table("bronze")
            .filter("topic = 'user_info'")
            .dropDuplicates()
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
            .select(F.sha2(F.concat(F.col("user_id"), F.lit("BEANS")), 256).alias("alt_id"),
                F.col('timestamp').cast("timestamp").alias("updated"),
                F.to_date('dob','MM/dd/yyyy').alias('dob'),
                'sex', 'gender','first_name','last_name',
                'address.*', "update_type")
            .writeStream
            .foreachBatch(batch_rank_upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{DA.paths.checkpoints}/users")
            .trigger(availableNow=True)
            .start()
            .awaitTermination())    

    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
        
    print(f"({int(time.time())-start} seconds)")

#     total = spark.read.table("ranked_updates").count()
#     print(f"...ranked_updates: {total} records)")

    total = spark.read.table("delete_requests").count()
    print(f"...delete_requests: {total} records)")

    total = spark.read.table("users").count()
    print(f"...users: {total} records)")
    
DA.process_users = _process_users
    
None # Suppressing Output

# COMMAND ----------

def age_bins(dob_col):
    from pyspark.sql import functions as F

    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

def _process_user_bins():
    import time
    from pyspark.sql import functions as F

    start = int(time.time())
    print(f"Processing user_bins table", end="...")
    
    (spark.table("users")
         .join(
            spark.table("user_lookup")
                .select("alt_id", "user_id"), 
            ["alt_id"], 
            "left")
        .select("user_id", 
                age_bins(F.col("dob")),
                "gender", 
                "city", 
                "state")
        .write
        .format("delta")
        .option("path", f"{DA.paths.user_db}/user_bins")
        .mode("overwrite")
        .saveAsTable("user_bins"))
    
    total = spark.read.table("user_bins").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    

DA.process_user_bins = _process_user_bins    

None # Suppressing Output

# COMMAND ----------

def _validate_count_less_than(df, expected):
    from pyspark.sql.dataframe import DataFrame    
    
    if type(df) == str: df = spark.read.table(df)
    elif type(df) != DataFrame: raise ValueError(f"Invalid parameter type ({type(df)}), expected str or DataFrame")
    
    total = df.count()
    assert total < expected, f"Expected less than {expected:,} records, found {total:,} records."    
    print(f"Found {total:,} records as expected.")
    return total

def _validate_count_greater_than(df, expected):
    from pyspark.sql.dataframe import DataFrame    
    
    if type(df) == str: df = spark.read.table(df)
    elif type(df) != DataFrame: raise ValueError(f"Invalid parameter type ({type(df)}), expected str or DataFrame")
    
    total = df.count()
    assert total > expected, f"Expected less than {expected:,} records, found {total:,} records."    
    print(f"Found {total:,} records as expected.")
    return total

def _validate_count(df, expected):
    from pyspark.sql.dataframe import DataFrame    
    
    if type(df) == str: df = spark.read.table(df)
    elif type(df) != DataFrame: raise ValueError(f"Invalid parameter type ({type(df)}), expected str or DataFrame")
    
    return _validate_total(expected, df.count(), "records")

def _validate_length(what, expected, name):
    return _validate_total(expected, len(what), name)

def _validate_total(expected, actual, name):
    assert expected == actual, f"Expected {expected:,} {name}, found {actual:,} {name}"
    print(f"Found {actual:,} {name} as expected.")
    return actual

