// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Conditionals

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will write conditional expressions using `if` statements.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 1
// MAGIC 
// MAGIC 1. Define a `val` named `a` with value `1` and another `val` named `b` with value `5`.
// MAGIC 2. Write a conditional expression that checks to see if `a` is less than `b`. 
// MAGIC 3. Based on the condition, assign the string literal `a is less than b` or `a is not less than b` to a variable named `result`. Print the value of `result`.

// COMMAND ----------

// ANSWER

// Define a val named a with value 1 and another val named b with value 5.

val a = 1 
val b = 5 

var result = ""

// Write a conditional expression that checks to see if a is less than b.
// Assign the string literal “a is less than b” or “a is not less than b” to a variable named result.

if ( a < b) {
  result = "a is less than b"  
} else {
  result = "a is not less than b"
}

println("The value of result: " + result)

// COMMAND ----------

// Test: Run this cell to test your solution.

var resultExpected = "a is less than b"
assert (result == resultExpected, s"Expected the result to be ${resultExpected} but found ${result}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 2
// MAGIC 
// MAGIC 1. Define a `val` named `c` with a value of `5`. 
// MAGIC 2. Write a conditional expression to check if `a < c`. Then, check if `b < c`. (`<` is the less-than operator)
// MAGIC 3. Based on the conditions, assign the string literal `a is less than c` or `b is less than c` to the variable `result`. Print the value of `result`.

// COMMAND ----------

//ANSWER

// Define a val named c to 5.

val c = 5 

result = ""

// Write a conditional expression to check if a < c. Then, check if b < c. (‘<’ is the less-than operator)
// Based on the conditions, assign the string literal “a is less than c” or “b is less than c” to a variable named result.
// Print the value of result.

if ( a < c) {
  result = "a is less than c"  
} else if ( b < c ) {
  result = "b is less than c"
}

println("The value of result: " + result)

// COMMAND ----------

// Test: Run this cell to test your solution.

var resultExpected = "a is less than c"
assert (result == resultExpected, s"Expected the result to be ${resultExpected} but found ${result}")

// COMMAND ----------

// MAGIC %md
// MAGIC # For Loops

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Generate a sequence of numbers from 1 to 10.
// MAGIC 2. Write a `for` loop to compute the sum of the sequence numbers.
// MAGIC 3. Write a `for` loop to sum all even numbers and odd numbers separately.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 3
// MAGIC 
// MAGIC Generate a sequence of numbers from 1 to 10 (excluding 10) and assign it to a `val` named `sequenceOfNumbers`.

// COMMAND ----------

// ANSWER

// Generate a sequence of numbers from 1 to 10 (excluding 10) 

val sequenceOfNumbers = 1 until 10 

// COMMAND ----------

// Test: Run this cell to test your solution.

var sequenceOfNumbersExpected = 1 until 10
assert (sequenceOfNumbers == sequenceOfNumbersExpected, s"Expected the result to be ${sequenceOfNumbersExpected} but found ${sequenceOfNumbers}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Exercise 4
// MAGIC 
// MAGIC Write a `for` loop to compute the sum of the all numbers in `sequenceOfNumbers` and store the result in a variable named `sumOfSequence`.

// COMMAND ----------

// ANSWER

var sumOfSequence = 0

// Write a for loop to compute the sum of the all numbers in sequenceOfNumbers

for (k <- sequenceOfNumbers) 
 sumOfSequence += k

println("Total sum is: " + sumOfSequence)

// COMMAND ----------

// Test: Run this cell to test your solution.

var sumOfSequenceExpected = 45
assert (sumOfSequence == sumOfSequenceExpected, s"Expected the result to be ${sumOfSequenceExpected} but found ${sumOfSequence}")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ##Exercise 5
// MAGIC 
// MAGIC 1. Write a `for` loop to compute the sum of the all **even** numbers in `sequenceOfNumbers` and store the result in a variable named `sumOfEvens` and sum of the all **odd** numbers in `sequenceOfNumbers` and store the result in a variable named `sumOfOdds`.
// MAGIC 2. Write this using only a **single** `for` loop.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the `%` (modulo) operator to see if there is a remainder when you divide a number by 2.

// COMMAND ----------

// ANSWER

var sumOfEvens = 0
var sumOfOdds = 0

// Write a for loop to compute the sum of the all eveb number and odd numbers and store them sumOfEvens and sumOfOdds respectively

for (k <- sequenceOfNumbers) {
  if (k % 2 == 0) {
    sumOfEvens += k
  } else {
    sumOfOdds += k
  }
}

println("Sum of even number is: " + sumOfEvens)
println("Sum of odd number is: " + sumOfOdds)

// COMMAND ----------

// Test: Run this cell to test your solution.

var sumOfEvensExpected = 20
var sumOfOddsExpected = 25
assert (sumOfEvens == sumOfEvensExpected, s"Expected the result to be ${sumOfEvensExpected} but found ${sumOfEvens}")
assert (sumOfOdds == sumOfOddsExpected, s"Expected the result to be ${sumOfOddsExpected} but found ${sumOfOdds}")

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
