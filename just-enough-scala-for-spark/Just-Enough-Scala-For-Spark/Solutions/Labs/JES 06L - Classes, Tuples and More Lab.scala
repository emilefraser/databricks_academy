// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Classes

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Define a class and create instances of it
// MAGIC 2. Define a case class and create instances of it

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC - Define a `SimpleTime` class with two attributes:
// MAGIC   - `hours`: an integer (in 24-hour format)
// MAGIC   - `minutes`: an integer
// MAGIC - Provide a constructor to set the attributes when creating an instance
// MAGIC   - Define a default value of 0 for `minutes`
// MAGIC - Provide a `getTime` method, which should return the time as a string in `HH:MM` format.  
// MAGIC - Create two instances of class:
// MAGIC   - One instance with `hours` as 6 and `minutes` as 30, assigned to the variable `morning`
// MAGIC   - One instance with `hours` as 18 (do not provide a value for `minutes`), assigned to the variable `evening`

// COMMAND ----------

// MAGIC %md
// MAGIC **NOTE:** Use `"%02d".format(...)` to add leading zeros as needed for formatting the result of `getTime`.

// COMMAND ----------

// ANSWER


// Define the class
class SimpleTime(val hours: Int, val minutes: Int = 0) {  
  def getTime() = "%02d:%02d".format(hours, minutes)
}


// Create instances morning and evening
var morning = new SimpleTime(6, 30)
var evening = new SimpleTime(18)

// COMMAND ----------

// Test: Run this cell to test your solution.

var morningTimeExpected = "06:30"
var eveningTimeExpected = "18:00"
assert (morning.getTime() == morningTimeExpected, s"Expected the result to be ${morningTimeExpected} but found ${morning.getTime()}")
assert (evening.getTime() == eveningTimeExpected, s"Expected the result to be ${eveningTimeExpected} but found ${evening.getTime()}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC - Define a case class named `StudentRecord` with a string attribute `name` and a double attribute `score`.
// MAGIC - Create an instance of a student record with the name `Robert` and a score of `70`, and assign the instance to a variable named `firstStudent`.

// COMMAND ----------

// ANSWER

// Define the case class
case class StudentRecord(name: String, score: Double)

// Crate an instance of the case class
val firstStudent = StudentRecord("Robert", 70)

// COMMAND ----------

// Test: Run this cell to test your solution.

val nameExpected = "Robert"
val scoreExpected = 70.0
assert (firstStudent.name == nameExpected, s"Expected the result to be ${nameExpected} but found ${firstStudent.name}")
assert (firstStudent.score == scoreExpected, s"Expected the result to be ${scoreExpected} but found ${firstStudent.score}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Create a list of students. This is done for you!

// COMMAND ----------

val listOfStudents = List(
  StudentRecord("Peter", 76.0),
  StudentRecord("Rahul", 45.0),
  StudentRecord("Bruce", 23.0),
  StudentRecord("Lisa", 77.0),
  StudentRecord("Naren", 54.0),
  StudentRecord("Wang", 62.0),
  StudentRecord("Jones", 10.0)
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Find the names of all students with a score over 50 in `listOfStudents` and assign the resulting list to the variable `selectedStudents`.

// COMMAND ----------

//ANSWER

// Filter the names
var selectedStudents = listOfStudents.filter( rec => rec.score > 70 ).map( rec => rec.name )

// COMMAND ----------

// Test: Run this cell to test your solution.

val selectedStudentsExpected = List("Peter", "Lisa")
assert (selectedStudents == selectedStudentsExpected, s"Expected the result to be ${selectedStudentsExpected} but found ${selectedStudents}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Congratulations! You have completed your lab successfully.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
