// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Functional Programming
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Start with a given list of student names in a class and scores they got on a test
// MAGIC 2. Make a correction in the scores across the board
// MAGIC 3. Filter out the students who have achieved more than a cutoff score
// MAGIC 4. Compute the average score by the class
// MAGIC 
// MAGIC All the exercises should be completed using functional programming features.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The student records are provided as a list of tuples. Each tuple contains the name of the student and their test score.

// COMMAND ----------

var studentsRecords = List(
  ("Peter", 76.0),
  ("Rahul", 45.0),
  ("Bruce", 23.0),
  ("Lisa", 77.0),
  ("Naren", 54.0),
  ("Wang", 62.0),
  ("Jones", 10.0)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 1
// MAGIC 
// MAGIC 1. Assuming that the professor determined that the difficulty level of the exam was high, they might decide to give 10 "grace marks" to the students.
// MAGIC 2. Add 10 to each student's score and assign the updated records to a new variable, `updatedStudentsRecords`.

// COMMAND ----------

//ANSWER

var  updatedStudentsRecords = studentsRecords.map(x => (x._1, x._2 + 10) )

// COMMAND ----------

// Test: Run this cell to test your solution.

var updatedStudentsRecordsExpected = List(
  ("Peter", 86.0),
  ("Rahul", 55.0),
  ("Bruce", 33.0),
  ("Lisa", 87.0),
  ("Naren", 64.0),
  ("Wang", 72.0),
  ("Jones", 20.0)
)
assert (updatedStudentsRecords == updatedStudentsRecordsExpected, s"Expected the result to be ${updatedStudentsRecordsExpected} but found ${updatedStudentsRecords}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Find the names of all students with a score over 50 in `updatedStudentsRecords` and assign the resulting list to the variable `selectedStudents`.

// COMMAND ----------

//ANSWER

var selectedStudents = updatedStudentsRecords.filter(x => x._2 > 50).map(x => x._1)

// COMMAND ----------

// Test: Run this cell to test your solution.

var selectedStudentsExpected = Vector("Peter", "Rahul", "Lisa", "Naren", "Wang")
assert (selectedStudents == selectedStudentsExpected, s"Expected the result to be ${selectedStudentsExpected} but found ${selectedStudents}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Calculate the average score of the class from `updatedStudentsRecords` and assign the result to the variable `averageScore`.

// COMMAND ----------

//ANSWER

var totalScore = updatedStudentsRecords.map(x => x._2).reduce(_ + _)
var averageScore = totalScore / updatedStudentsRecords.size

// COMMAND ----------

// Test: Run this cell to test your solution.

var averageScoreExpected = 59.57
// rounding off the results to two decimal places for comparison
averageScore = BigDecimal(averageScore).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
assert (averageScore == averageScoreExpected, s"Expected the result to be ${averageScoreExpected} but found ${averageScore}")

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
