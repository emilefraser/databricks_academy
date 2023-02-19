// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Methods

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Write a method that takes two double values as arguments and returns a Boolean value indicating whether the first parameter is greater than the second.
// MAGIC 2. Write a method that takes weight and height, calculates BMI, and then returns an **advisory message**.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 1
// MAGIC 
// MAGIC 1. Create a method `isArg1GreaterThanArg2` that takes two arguments of type double.
// MAGIC 2. Return `true` if the first argument is greater than the second and `false` otherwise.
// MAGIC 3. Invoke the method with following values:
// MAGIC   - Arguments of 10.0 and 20.0 and assign the returned value to a variable named `firstResult`
// MAGIC   - Arguments of 30.0 and 20.0 and assign the returned value to a variable named `secondResult`

// COMMAND ----------

// TODO

// Define a method **isArg1GreaterThanArg2** that takes two Double arguments.

FILL_IN

// Invoke the method with values 10.0 and 20.0 and assign the result to variable firstResult
var firstResult = FILL_IN

// Invoke the method with values 30.0 and 20.0 and assign the result to variable secondResult
val secondResult = FILL_IN

// COMMAND ----------

// Test: Run this cell to test your solution.

var firstResultExpected = false
var secondResultExpected = true
assert (firstResult == firstResultExpected, s"Expected the result to be ${firstResultExpected} but found ${firstResult}")
assert (secondResult == secondResultExpected, s"Expected the result to be ${secondResultExpected} but found ${secondResult}")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 2
// MAGIC 
// MAGIC 1. Create a method `getBMIStats` that takes the following three arguments
// MAGIC   - `weight` - weight of a person in **lbs**. Data type is double.
// MAGIC   - `height` - height of a person in **inches**. Data type is double.
// MAGIC   - `bmiCutoff` - BMI cutoff, beyond which the person is considered overweight. Data type is double. This should be defined as an optional parameter and initialized to `25.0`.
// MAGIC 2. The method should calculate BMI using the formula `(703 x weight) / (height x height)`. 
// MAGIC 3. The method should return an advisory message of type string with value the **Normal** if the BMI is less than `bmiCutoff`, or else return the string value **Overweight**.
// MAGIC 4. Invoke the method with following values for two persons:
// MAGIC   - Person 1: weight 140 lbs and height 65 inches, and assign the returned value to a variable `statusFirst`
// MAGIC   - Person 2: weight 200 lbs and height 70 inches, and assign the returned value to a variable `statusSecond`

// COMMAND ----------

// TODO

var statusFirst, statusSecond: String = "No result" // Declaring and initializing both the variables in one line

// Write the method getBMIStats 
FILL_IN

// Invoke the method with weight 140 lbs and height 65 inches and assign the returned value to a variable statusFirst
FILL_IN = FILL_IN

// Invoke the method with weight 200 lbs and height 70 inches and assign the returned value to a variable statusSecond
FILL_IN = FILL_IN

println("Person 1 - Advisory: " + statusFirst)
println("Person 2 - Advisory: " + statusSecond)

// COMMAND ----------

// Test: Run this cell to test your solution.

var statusFirstExpected = "Normal"
var statusSecondExpected = "Overweight"

assert (statusFirst == statusFirstExpected, s"Expected the result to be ${statusFirstExpected} but found ${statusFirst}")
assert (statusSecond == statusSecondExpected, s"Expected the result to be ${statusSecondExpected} but found ${statusSecond}")

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
