# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="1.5"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

def create_gym_logs():
    import time
    
    start = int(time.time())
    print(f"Creating gym_mac_logs dataset", end="...")

    DA.hidden.gym_mac_logs_source = f"{DA.hidden.datasets}/gym-logs"
    DA.paths.gym_mac_logs_json = f"{DA.paths.working_dir}/gym_mac_logs.json"

    # Copies files to demo directory
    files = dbutils.fs.ls(DA.hidden.gym_mac_logs_source)
    # All files except those in 2019-12-10 where 2019-12-0 includes 1-9
    for curr_file in [file.name for file in files if file.name.startswith(f"2019120")]:
        # print(f"...adding file {curr_file}")
        dbutils.fs.cp(f"{DA.hidden.gym_mac_logs_source}/{curr_file}", f"{DA.paths.gym_mac_logs_json}/{curr_file}")
        
    print(f"({int(time.time())-start} seconds)")

# COMMAND ----------

class GymMacStreamingFactory:
    def __init__(self):
        self.curr_day = 10
        self.target = DA.paths.gym_mac_logs_json
        self.source = DA.hidden.gym_mac_logs_source
    
    def load_day(self, files):
        files = [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]
        for curr_file in files:
            dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.target}/{curr_file}")
        return len(files)
    
    def load(self, continuous=False):
        import time
        
        total = 0
        start = int(time.time())
        files = dbutils.fs.ls(self.source)
        
        if self.curr_day > 16:
            print("Data source exhausted\n")
            
        elif continuous == True:
            print(f"Loading all gym_mac_log files", end="...")
            while self.curr_day <= 16:
                total += self.load_day(files)
                self.curr_day += 1
            
        else:
            print(f"Loading gym_mac_logs for day #{self.curr_day}", end="...")
            total = self.load_day(files)
            self.curr_day += 1
            
        print(f"({int(time.time())-start} seconds, {total:,} files)")

# COMMAND ----------

DA.cleanup()
DA.init()

create_gym_logs()
DA.gym_mac_stream = GymMacStreamingFactory()

DA.conclude_setup()

