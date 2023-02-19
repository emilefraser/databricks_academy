// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Methods or Functions

// COMMAND ----------

// MAGIC %md
// MAGIC * Methods are defined using `def`
// MAGIC * Parameter data types **must** be specified
// MAGIC * Parameters can be made optional if initialized in the definition
// MAGIC * The return type is usually optional
// MAGIC   * It can be inferred at compile-time (unless the function is recursive)
// MAGIC   * Best practice is to declare the return type for better compile-time type safety
// MAGIC * Separate the signature from the function body with `=`
// MAGIC * Braces are not needed if the function body is just a single expression
// MAGIC * The result of the last expression evaluated in the method is the return value
// MAGIC   * A `return` keyword is supported but not necessary

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 1

// COMMAND ----------

def multiplyByTwo(x: Int): Int = {
  println("Inside multiplyByTwo")
  x * 2  // Return value
}

// Braces not needed if the body is a simple expression
def add(i: Int, j: Int) = i + j

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Invoking the method.

// COMMAND ----------

val result = multiplyByTwo(3)
println(result)

val answer = add(23, 19)
println(answer)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 2
// MAGIC 
// MAGIC Method parameters can be optional, if initialized in the method signature.

// COMMAND ----------

def optSum(i: Int, j: Int = 100) = {
  i + j  // j defaults to 100 if omitted
}

// invoking the method by passing only one parameter
optSum(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 3

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * If a method does not return anything, then explicitly declare the return type as `Unit`
// MAGIC * The `Unit` type consists of only one value, called the *unit value*, which is represented by empty parentheses (`()`) or braces (`{}`)
// MAGIC * By convention, `Unit` is used to represent the result of an expression that performs a side-effect, such as:
// MAGIC   * Printing with `print()` or `println()`
// MAGIC   * Variable or value assignment
// MAGIC   * Looping with `while`
// MAGIC * It simply means that the method returned successfully
// MAGIC * It's basically the same as a return type of `void` for functions in languages such as C++ or Java

// COMMAND ----------

def printMessage(message: String): Unit = {
  println(message)
}

// Invoking the method  
val result = printMessage("Hello World!")
print("Result: " + result)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 4

// COMMAND ----------

// MAGIC %md
// MAGIC If you declare a method with an empty parameter list, you can invoke it either with an empty argument list or without any argument list:

// COMMAND ----------

def greet(): Unit = println("Hello")

// Invoking with an empty argument list
greet()

// Invoking without an argument list
greet

// COMMAND ----------

// MAGIC %md
// MAGIC If you declare a method without a parameter list, you must omit the argument list when you invoke it:

// COMMAND ----------

def answer:Int = {
  2 * 21
}

// Invoking without an argument list works
val result = answer

// Invoking with an empty argument list results in an error
// val result = answer()

// COMMAND ----------

// MAGIC %md
// MAGIC The Scala style convention is:
// MAGIC 
// MAGIC * If a parameterless method acts as an accessor (that is, a simple "getter" method), declare it without a parameter list and invoke it without an argument list
// MAGIC * If a parameterless method performs a side effect, declare it with an empty parameter list and invoke it with an empty argument list
// MAGIC 
// MAGIC See <a href="https://docs.scala-lang.org/style/naming-conventions.html#parentheses" target="_blank">https&#58;//docs.scala-lang.org/style/naming-conventions.html#parentheses</a>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Useful built-in method: `assert()`
// MAGIC 
// MAGIC The `assert()` method takes a `Boolean` expression and a `String` message. If the expression is false it stops executing code further, throws an error, and prints the message.
// MAGIC You will see use of this method throughout our notebooks to verify if the output of our data pipelines are as expected or not.

// COMMAND ----------

assert(optSum(10) == 110, "Sum is not correct")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Packages and Imports
// MAGIC 
// MAGIC A package is an organizational unit that can contain entities such as classes, objects, and other packages.
// MAGIC 
// MAGIC Entities that are contained in a given package belong to that package's _namespace_.
// MAGIC 
// MAGIC For more information, see <a href="https://en.wikibooks.org/wiki/Scala/Packages" target="_blank">https&#58;//en.wikibooks.org/wiki/Scala/Packages</a>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Classes, objects, and static methods can all be imported using `import`
// MAGIC - Underscore (`_`) can be used as a wildcard to import everything from a particular context
// MAGIC - Scala `import` supports aliasing

// COMMAND ----------

import scala.math.log  // Import only the log method from the scala.math package
import scala.math._    // Import all methods from the scala.math package
import scala.math.{log => logE}  // Import log and give it the alias logE

// COMMAND ----------

// MAGIC %md
// MAGIC Scala automatically imports the contents of:
// MAGIC 
// MAGIC * **`java.lang`**
// MAGIC * **`scala`**
// MAGIC * **`scala.Predef`**

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
