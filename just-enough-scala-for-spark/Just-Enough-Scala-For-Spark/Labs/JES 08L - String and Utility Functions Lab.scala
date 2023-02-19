// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC Convert the given string to lowercase.

// COMMAND ----------

// TODO

val aString = "Tomorrow is another Day"

// Convert the string to lower case
val lowerCaseStr = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

val lowerCaseStrExpected = "tomorrow is another day"
assert (lowerCaseStr == lowerCaseStrExpected, s"Expected the result to be ${lowerCaseStrExpected} but found ${lowerCaseStr}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Split the string in `lowerCaseStr` using a space separator to create a list of words, and assign the result to a variable named `wordsList`.

// COMMAND ----------

// TODO

// Split the string by space separator
val wordsList = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

val wordsListExpected = Vector("tomorrow", "is", "another", "day")
assert (wordsList.toVector == wordsListExpected, s"Expected the result to be ${wordsListExpected} but found ${wordsList.toVector}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Join the list of words in the variable `wordsList` into a string where words are separated by a comma (","), and assign the result to the variable `newString`.

// COMMAND ----------

// TODO

// Join the list of words into a string with words separated by ","
val newString = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

val newStringExpected = "tomorrow,is,another,day"
assert (newString == newStringExpected, s"Expected the result to be ${newStringExpected} but found ${newString}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 4
// MAGIC 
// MAGIC Find the absolute value of the variable `aNumber` and assign the result to `absoluteValue`. Use math library functions.

// COMMAND ----------

// TODO

// import math library
FILL_IN

val aNumber = -23.45

// Find the absolute value
val absoluteValue = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

val absoluteValueExpected = 23.45
assert (absoluteValue == absoluteValueExpected, s"Expected the result to be ${absoluteValueExpected} but found ${absoluteValue}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 5
// MAGIC 
// MAGIC Calculate the base-10 logarithm of the variable `anotherNumber` and assign it to the variable `logValue`.

// COMMAND ----------

// TODO

val anotherNumber = 100.0

// Find the log value
val logValue = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

val logValueExpected = 2.0
assert (logValue == logValueExpected, s"Expected the result to be ${logValueExpected} but found ${logValue}")

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
