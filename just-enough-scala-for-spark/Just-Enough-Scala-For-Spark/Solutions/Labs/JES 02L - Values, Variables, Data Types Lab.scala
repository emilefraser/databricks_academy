// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Values, Variables, Data Types: Lab

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Define a **`val`** and initialize it with a constant value
// MAGIC 2. Try to change the value of a **`val`** after initialization and check what happens
// MAGIC 3. Define variables (**`var`**) with and without specifiying data types
// MAGIC 4. Verify what happens if you define a variable of one data type and assign a value of another data type 
// MAGIC 5. Create an expression using **`val`**s and **`var`**s

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> At the end of each exercise is a test cell that you should run to see if you solved the exercise correctly. These test cells use the **`assert()`** function to check a Boolean condition. If the condition is **`false`**, then **`assert()`** throws an **`AssertionError`** (with a custom error message, if provided). If the condition is **`true`**, then **`assert()`** has no effect.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 1
// MAGIC 
// MAGIC Define a **`val`** named **`bmiConstant`** with a value of **`703.07`**.
// MAGIC 
// MAGIC Print the data type of the variable.

// COMMAND ----------

// ANSWER

val bmiConstant = 703.07

// Print the data type of the variable
println("Data type of bmiConstant: " + bmiConstant.getClass())

// COMMAND ----------

// Test: Run this cell to test your solution.

val bmiConstantExpected = 703.07
assert(bmiConstant == bmiConstantExpected, s"Expected the bmiConstant to be ${bmiConstantExpected} but found ${bmiConstant}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 2
// MAGIC 
// MAGIC Define a variable named **`height`** with an initialial value of **`68`**.
// MAGIC 
// MAGIC Print the data type of the variable.

// COMMAND ----------

// ANSWER

var height = 68
println("Data type of height: " + height.getClass())

// COMMAND ----------

// Test: Run this cell to test your solution.

val heightExpected = 68
assert(height == heightExpected, s"Expected the bmiConstant to be ${heightExpected} but found ${height}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 3
// MAGIC 
// MAGIC Define a variable named **`lbs`**, explicity setting the data type to **`Double`**, and initialize its value to **`150.0`**.
// MAGIC 
// MAGIC Print the data type of the variable.

// COMMAND ----------

// ANSWER

var lbs: Double = 150.0
println("Data type of lbs: " + lbs.getClass())

// COMMAND ----------

// Test: Run this cell to test your solution.

val lbsExpected = 150.0
assert(lbs == lbsExpected, s"Expected the bmiConstant to be ${lbsExpected} but found ${lbs}")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 4
// MAGIC Define a variable named **`trueOrFalse`**
// MAGIC , explicity setting the data type to **`Boolean`**
// MAGIC and initialize its value to
// MAGIC **`true`**.
// MAGIC 
// MAGIC Print the data type of the variable.

// COMMAND ----------

// ANSWER

var trueOrFalse: Boolean = true
println("Data type of trueOrFalse: " + trueOrFalse.getClass())

// COMMAND ----------

// Test: Run this cell to test your solution.

val booleanExpected:Boolean = true
assert(trueOrFalse == booleanExpected, s"Expected the bmiConstant to be ${booleanExpected} but found ${trueOrFalse}")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ##Exercise 5
// MAGIC 
// MAGIC Write an expression for computing body mass index (BMI) from height and weight.
// MAGIC 
// MAGIC Assign the result to a variable named **`bmi`**.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** BMI is computed by dividing **`lbs`** by the square value of **`height`**, and then multiplying the result of that of by **`bmiConstant`**.

// COMMAND ----------

// ANSWER

var bmi = lbs / (height * height) * bmiConstant

// COMMAND ----------

// Test: Run this cell to test your solution.

val bmiExpected = 22.807201557093425
assert(scala.math.abs(bmi - bmiExpected) < 0.00001, s"Expected the bmiConstant to be ${bmiExpected} but found ${bmi}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 6
// MAGIC 
// MAGIC Try to change the value of **`height`** to **`150.5`** and check what happens.
// MAGIC 
// MAGIC Try to change the value of **`trueOrFalse`** to the string literal **`maybe`** and check what happens.

// COMMAND ----------

// ANSWER

// The lines of code below should be uncommented to produce the expected error.
// height = 150.5
// trueOrFalse = "maybe"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 7
// MAGIC 
// MAGIC Try to change the value of **`bmiConstant`** to **`708`** and check what happens.

// COMMAND ----------

// ANSWER

// The line of code below should be uncommented to produce the expected error.
// bmiConstant = 708

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Congratulations! You have completed your first lab.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
