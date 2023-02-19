// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Exceptions

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Create and use a custom exception class.
// MAGIC 2. Use `try`/`catch` to handle an exception

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC Define a custom exception class named `InvalidBodyParametersException`.

// COMMAND ----------

// TODO

// Define a custom exception
FILL_IN 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC 1. Encapsulate the code block in the method with an appropriate `try`-`catch` structure.
// MAGIC 2. Check if `height` or `weight` is greater than 0. If not throw an `InvalidBodyParametersException` exception with a message "Height or weight can not be zero or negative values."
// MAGIC 3. The method should return the advisory message "INVALID INPUTS" if an exception occurs.

// COMMAND ----------

// TODO

def getBMIStats(weight: Double, height: Double, bmiCutoff: Double = 25.0): String = {
  
 var advisory: String = ""
  
// Add try-catch
FILL_IN
  
// Add code to check if height or weight are more than zero and throw an exception if not
FILL_IN
  
var bmi = (703.0 * weight) / (height * height)
if (bmi > bmiCutoff) {
  advisory = "Overweight"
} else {
  advisory = "Normal"
}
    
return advisory
  
// Add catch statements
FILL_IN
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 3: Checking Scenarios
// MAGIC 
// MAGIC 1. Invoke the method with valid height of 70 inches and weight of 140 lbs and assign the result to the variable `firstAdvisory`.
// MAGIC 2. Invoke the method with invalid height of 0.00 inches and weight of 140 lbs and assign the result to the variable `secondAdvisory`.
// MAGIC 3. Invoke the method with invalid height of 60.0 inches and weight of -1.0 lbs and assign the result to the variable `thirdAdvisory`.

// COMMAND ----------

// TODO

var firstAdvisory = FILL_IN
var secondAdvisory = FILL_IN
var thirdAdvisory = FILL_IN

// COMMAND ----------

// Test: Run this cell to test your solution.

var firstAdvisoryExpected = "Normal"
var secondAdvisoryExpected = "INVALID INPUTS"
var thirdAdvisoryExpected  = "INVALID INPUTS"

assert (firstAdvisory == firstAdvisoryExpected, s"Expected the result to be ${firstAdvisoryExpected} but found ${firstAdvisory}")
assert (secondAdvisory == secondAdvisoryExpected, s"Expected the result to be ${secondAdvisoryExpected} but found ${secondAdvisory}")
assert (thirdAdvisory == thirdAdvisoryExpected, s"Expected the result to be ${thirdAdvisoryExpected} but found ${thirdAdvisory}")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
