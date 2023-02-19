// Databricks notebook source
// MAGIC %md
// MAGIC This script provides a custom Streaming Query Listener that log progress as JSON files. 
// MAGIC 
// MAGIC For more information see <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#reporting-metrics-programmatically-using-asynchronous-apis" target="_blank">the Structured Streaming Programming Guide, Reporting Metrics Programmatically-using Asynchronous APIs</a>
// MAGIC 
// MAGIC To reset the target logging directory, change the calling cell to **`%run ../Includes/StreamingQueryListener $reset="true"`**

// COMMAND ----------

dbutils.widgets.text("reset", "false")
val reset = dbutils.widgets.get("reset")

if(reset=="true") {
  dbutils.fs.rm(spark.conf.get("da.paths.streaming_logs_json"), true)
}

// COMMAND ----------

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming._

import java.io.File

class CustomListener() extends StreamingQueryListener {

  // Sink
  private val fileDirectory = spark.conf.get("da.paths.streaming_logs_json").replaceAll("dbfs:/", "/dbfs/")

  // Modify StreamingQueryListener Methods 
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
  }

  // Send Query Progress metrics to DBFS 
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    try {
      
      val file = new File(s"${fileDirectory}/${event.progress.name}_${event.progress.id}_${event.progress.batchId}.json")
      println(s"Writing $file")
      
      val result_touch = FileUtils.touch(file)
      println(s"  Touch: $result_touch")
      
      val result_write = FileUtils.writeStringToFile(file, event.progress.json)
      println(s"  Write: $result_write")
      println("-"*80)
      
    } catch {
      case e: Exception => println(s"Failed to print\n$e")
    }
  }
}

dbutils.fs.mkdirs(spark.conf.get("da.paths.streaming_logs_json"))

val streamingListener = new CustomListener()
spark.streams.addListener(streamingListener)

