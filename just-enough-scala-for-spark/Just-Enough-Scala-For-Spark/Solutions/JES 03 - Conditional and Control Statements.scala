// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conditional Statements: if-else
// MAGIC 
// MAGIC Scala supports an **`if-else`** structure for conditional evaluation.

// COMMAND ----------

// if as a statement
if (1 > 0) {
  println("It's true!")
}

// if-else as a statement
val z: Boolean = false

if (z) {
  println("It's true!")
} else {
  println("It's false")
}

// if-else as a conditional expression
val result: String = if (3 < 4) "Math is working correctly" else "Math is broken"
println(result)
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conditional Statements: if-elseif-else
// MAGIC 
// MAGIC If-elseif-else allows your program to check for more than one specific condition. Your program will test each condition in order. Once an `if` or an `else if` succeeds, none the the remaining conditions are tested. If none of the listed conditions succeeds and an `else` block exists, the code block following `else` will execute. If there is no `else` block, none of the code blocks will exceute. Multiple `else if` statements can be added after the the initial `if` statement  Notice, in the example, that the syntax remains the same. Each condition is wrapped in parentheses, and the resulting code block is wrapped in curly braces. 

// COMMAND ----------

// Change the value of the variable so that each line of the
// if-elseif-else block runs.

var testValue = 5

// if as a statement
if (testValue == 1) {
  println("The testValue is 1")
} else if (testValue == 2) {
  println("The testValue is 2")
} else if (testValue == 3) {
  println("The testValue is 3")
} else {
  println("The testValue is not 1, 2, or 3")
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Generating Sequence Numbers

// COMMAND ----------

// MAGIC %md
// MAGIC The following Scala expressions create sequences of integers:

// COMMAND ----------

// A range including the final value 9
val listOfNumbersA = 0 to 9        

// A range excluding the final value 10
val listOfNumbersB = 0 until 10    

// An inclusive range with an increment of 2
val listOfNumbersC = 0 to 10 by 2  

// An inclusive range in descending order with a decrement of 2
val listOfNumbersD = 10 to 0 by -2 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The syntax used above is short-hand for the more traditional "dot" style seen below:
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Scala methods that take a single parameter can be invoked without dots or parentheses

// COMMAND ----------

// A range including the final value 9
val listOfNumbersA = 0.to(9)        

// A range excluding the final value 10
val listOfNumbersB = 0.until(10)

// An inclusive range with an increment of 2
val listOfNumbersC = 0.to(10).by(2)

// An inclusive range in descending order with a decrement of 2
val listOfNumbersD = 10.to(0).by(-2)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Control Flow Statments (Loops)
// MAGIC 
// MAGIC For looping, we can use either **`for`** or **`while`** loops. 
// MAGIC 
// MAGIC In **`for`** loops, the loop cycles through a range of numbers or collection of elements (to be discussed in a later section), assigning each value in turn to the index variable provided.

// COMMAND ----------

// A numerical range
for (k <- 1 until 5) 
  println("Value of k: " + k)


// A List of values
for (name <- List("Ari", "Basia", "Chao", "Daru"))
  println(name)

// COMMAND ----------

// MAGIC %md
// MAGIC - A **`while`** loop statement is executed until the condition is false. 

// COMMAND ----------

var k = 0

while (k < 5) {
  println("Value of k: " + k)
  k += 1  // Update k by adding 1
}
println("-" * 80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
