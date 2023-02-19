# Databricks notebook source
# MAGIC 
# MAGIC %run ./classic-setup $course="multihop" $mode="reset"

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# COMMAND ----------

source_dir = "/mnt/training/healthcare"

streamingPath          = userhome + "/source"
bronzePath             = userhome + "/bronze"
recordingsParsedPath   = userhome + "/silver/recordings_parsed"
recordingsEnrichedPath = userhome + "/silver/recordings_enriched"
dailyAvgPath           = userhome + "/gold/dailyAvg"

checkpointPath               = userhome + "/checkpoints"
bronzeCheckpoint             = userhome + "/checkpoints/bronze"
recordingsParsedCheckpoint   = userhome + "/checkpoints/recordings_parsed"
recordingsEnrichedCheckpoint = userhome + "/checkpoints/recordings_enriched"
dailyAvgCheckpoint           = userhome + "/checkpoints/dailyAvgPath"

# COMMAND ----------

class FileArrival:
  def __init__(self):
    self.source = source_dir + "/tracker/streaming/"
    self.userdir = streamingPath + "/"
    self.curr_mo = 1
    
  def arrival(self, continuous=False):
    if self.curr_mo > 12:
      print("Data source exhausted\n")
    elif continuous == True:
      while self.curr_mo <= 12:
        curr_file = f"{self.curr_mo:02}.json"
        dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
        self.curr_mo += 1
    else:
      curr_file = f"{str(self.curr_mo).zfill(2)}.json"
      dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
      self.curr_mo += 1
      
NewFile = FileArrival()

