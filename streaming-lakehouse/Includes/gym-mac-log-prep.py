# Databricks notebook source
# MAGIC 
# MAGIC %run ./common-setup $course="gym" $mode="reset"

# COMMAND ----------

# MAGIC %run ./data-source

# COMMAND ----------

source = f"{URI}/gym-logs/"
raw = userhome + "/raw/"
files = dbutils.fs.ls(source)

dbutils.fs.rm(raw, True)

for curr_file in [file.name for file in files if file.name.startswith(f"2019120")]:
    dbutils.fs.cp(source + curr_file, raw + curr_file)

# COMMAND ----------

class FileArrival:
    def __init__(self, source, userdir):
        self.source = source
        self.userdir = userdir
        self.curr_day = 10
    
    def arrival(self, continuous=False):
        files = dbutils.fs.ls(self.source)
        if self.curr_day > 16:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_day <= 16:
                for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                    dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
                self.curr_day += 1
        else:
            for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
            self.curr_day += 1

# COMMAND ----------

NewFile = FileArrival(source, raw)

# COMMAND ----------

gymMacLogs = f"{userhome}/mac_logs"
gymMacLogsCheckpoint = f"{userhome}/mac_logs_checkpoint"

