# Databricks notebook source
class Paths():
    def __init__(self, working_dir, clean_lesson):
        self.working_dir = working_dir
        self.user_db = f"{working_dir}/{clean_lesson}.db"
        self.checkpoints = f"{working_dir}/_checkpoints"    
            
    def exists(self, path):
        try: return len(dbutils.fs.ls(path)) >= 0
        except Exception:return False

    def print(self, padding="  "):
        max_key_len = 0
        for key in self.__dict__: max_key_len = len(key) if len(key) > max_key_len else max_key_len
        for key in self.__dict__:
            label = f"{padding}DA.paths.{key}:"
            print(label.ljust(max_key_len+13) + DA.paths.__dict__[key])
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{","").replace("}","").replace("'","")
    
class DBAcademyHelper():
    def __init__(self, lesson):
        import re, time
        
        assert lesson is not None, f"The lesson must be specified"

        self.initialized = False
        self.start = int(time.time())
        
        self.course_name = "adewd"          # Name should be the full name, not the code.
        self.course_code = self.course_name # Hacking this as a temporary solution
        
        self.lesson = lesson.lower()

        # Define username
        self.username = spark.sql("SELECT current_user()").first()[0]
        clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)

        self.db_name_prefix = f"dbacademy_{clean_username}_{self.course_name}"
        # self.source_db_name = None

        self.working_dir_prefix = f"dbfs:/user/{self.username}/dbacademy/{self.course_name}"
        
        clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson)
        working_dir = f"{self.working_dir_prefix}/{self.lesson}"
        self.paths = Paths(working_dir, clean_lesson)
        self.hidden = Paths(working_dir, clean_lesson)
        self.db_name = f"{self.db_name_prefix}_{clean_lesson}"

        self.hidden.datasets = f"{self.working_dir_prefix}/datasets"
            
    def init(self, create_db=True):
        spark.catalog.clearCache()
        self.create_db = create_db
        
        if create_db:
            print(f"\nCreating the database \"{self.db_name}\"")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
            spark.sql(f"USE {self.db_name}")
            
        self.initialized = True

    def cleanup(self):
        for stream in spark.streams.active:
            print(f"Stopping the stream \"{stream.name}\"")
            try: stream.stop()
            except: pass # Bury any exceptions
            try: stream.awaitTermination()
            except: pass # Bury any exceptions
        
        if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database \"{self.db_name}\"")
            spark.sql(f"DROP DATABASE {self.db_name} CASCADE")
        
        if self.paths.exists(self.paths.working_dir):
            print(f"Removing the working directory \"{self.paths.working_dir}\"")
            dbutils.fs.rm(self.paths.working_dir, True)
        
        # FIXME: Commented out because it might break during parallel testing.
        # self.databricks_api('POST', '2.0/secrets/scopes/delete', on_error="ignore", scope="DA-ADE3.03")
        
        # Make sure that they were not modified
        if self.initialized: validate_datasets()

    def conclude_setup(self, validate=True):
        import time
        
        spark.conf.set("da.db_name", self.db_name)
        for key in self.paths.__dict__:
            spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        
        print("\nPredefined Paths:")
        DA.paths.print()

        if self.create_db:
            print(f"\nPredefined tables in {self.db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")
                
        print()
        if validate: validate_datasets()
        print(f"\nSetup completed in {int(time.time())-self.start} seconds")

    @staticmethod
    def databricks_api(http_method, path, *, on_error="raise", **data):
        """
        Invoke the Databricks REST API for the current workspace as the current user.
        
        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            on_error: 'raise', 'ignore', or 'return'.
                'raise'  means propogate the HTTPError (Default)
                'ignore' means return None
                'return' means return the error message as parsed json if possible, otherwise as text.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            requests.HTTPError: If the API returns an error and on_error='raise'.
        """
        import requests, json
        url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)+"/api/"
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
        web = requests.Session()
        web.headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'text/json'}
        if http_method == 'GET':
            params = {k: str(v).lower() if isinstance(value, bool) else v for k,v in data.items()}
            resp = web.request(http_method, url + path, params = params)
        else:
            resp = web.request(http_method, url + path, data = json.dumps(data))
        if on_error.lower() in ["raise", "throw", "error"]:
            resp.raise_for_status()
        elif on_error.lower() in ["ignore", "none"] and not (200 <= resp.status_code < 300):
            return None
        elif on_error.lower() not in ["return", "return_error"]:
            raise Exception("on_error argument must be one of 'raise', 'ignore' or 'return'")
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            return resp.body

    def get_username_hash(self):
        """
        Utility method to split the user's email address, dropping the domain, and then creating a hash based on the full email address and course_code. The primary usage of this function is in creating the user's database, but is also used in creating SQL Endpoints, DLT Piplines, etc - any place we need a short, student-specific name.
        """
        da_name = self.username.split("@")[0]                                   # Split the username, dropping the domain
        da_hash = abs(hash(f"{self.username}-{self.course_code}")) % 10000      # Create a has from the full username and course code
        return da_name, da_hash
        
          
dbutils.widgets.text("lesson", "None")
lesson = dbutils.widgets.get("lesson")
DA = DBAcademyHelper(None if lesson == "None" else lesson)

# Address the larger stability of this course by setting this value globally.
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

