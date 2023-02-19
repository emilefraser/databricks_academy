// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Exceptions

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Exception handling is key to write robust and fail-safe code. 
// MAGIC - There may be disruptions in code flow if there is an unexpected exception or failure in the code. 
// MAGIC - The code should be prepared to deal with expected and unexpected exceptions, recover from them, and allow the code flow to continue.

// COMMAND ----------

// MAGIC %md
// MAGIC Scala has two main mechanisms to implement exception handling: 
// MAGIC 
// MAGIC 1. **`try`**/**`catch`** - Similar to Java exception handling mechanism
// MAGIC 2. **`scala.util.Try`** - We will discuss in subsequent section
// MAGIC 
// MAGIC There are other alternative too!
// MAGIC 
// MAGIC Let's start with an example of **`try`**/**`catch`**. 
// MAGIC 
// MAGIC The code block is wrapped in **`try`** and the exceptions are optionally caught with one or more **`catch`**. 
// MAGIC 
// MAGIC The **`catch`** block can handle multiple exceptions with **`case`** statments.
// MAGIC 
// MAGIC ```
// MAGIC   try {
// MAGIC     // code block 
// MAGIC   } catch {
// MAGIC     case e: CustomException => // do something
// MAGIC     case e: Exception => // do something
// MAGIC     case _: Throwable => // do something
// MAGIC   } finally {
// MAGIC     cleanup() // for example, close database connection or close a file etc.
// MAGIC   }
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Example 1: Using Try/Catch for dealing with Exceptions
// MAGIC 
// MAGIC This example demonstrates a simple **`try`**/**`catch`** pattern. 
// MAGIC 
// MAGIC Let's say we have scenario as below.
// MAGIC 
// MAGIC - A program reads an input as a string, which represents an age field
// MAGIC - The string value should be numeric
// MAGIC - Write a method to convert the string field to numeric
// MAGIC - If not numeric then catch the exception and set the field to -1.0 to represent missing

// COMMAND ----------

// Define the method which takes a string and returns a double value

def validateAge(age:String): Double = {
  
  try{
    // Convert the parameter to a double
    age.toDouble   
    
  } catch {
    // Catch any exception that might be thrown
    // In most cases it's because "age" is not a number
    case e: Exception => {
      e.printStackTrace()
      return -1.0
    }
  }
  finally {
    // not utilized. But can be used in other scenarios
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Next, invoke the method with different values to test it.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Test scenario 1: Valid Age
// MAGIC 
// MAGIC This should execute without generating any exceptions.

// COMMAND ----------

var age = validateAge("10")  
println( "Age is: " + age )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Test scenario 2: Invalid Format (Not a Number)
// MAGIC 
// MAGIC Calling the method with the argument "as" throws an exception, which the method catches, and results in a return value of -1.0.

// COMMAND ----------

// Calling with a string literal
var age = validateAge("as")  
println( "Age is: " + age )
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 2: Handling Multiple Exception Scenarios
// MAGIC 
// MAGIC Let's say 
// MAGIC 
// MAGIC - the field validation contains a rule that the age should be more than 18 and less than 100. 
// MAGIC - if the age does not fall within this range, a invalid data exception should be thrown. 
// MAGIC 
// MAGIC To implement this, a custom exception class can be created.

// COMMAND ----------

class InvalidDataException(message: String) extends Exception(message) {
  // no code here 
}

// COMMAND ----------

// MAGIC %md
// MAGIC To validate the field, the field will need to pass through two tests:
// MAGIC   1. If the field is numeric field or not. This will throw a **`NumberFormatException`** if not numeric and return the age as -1.0 to represent a missing field.
// MAGIC   2. If the value is between 18 and 100 or not. If not, this will throw an **`InvalidDataException`** and return the age as -1.0 to represent a missing field.
// MAGIC 
// MAGIC Define a new method called **`validateAgeAdvaced()`**. 
// MAGIC 
// MAGIC To deal with multiple exception scenarios, the **`catch`** block can be **`case`** statements for different exceptions thrown.

// COMMAND ----------

// define the new method
def validateAgeAdvanced(age:String): Double = {
  
  try{
       
    // convert to number
    var _age = age.toDouble   

    // change the range of the field
    if (_age < 18.0 | _age > 100.0) {
      // throw an exception with invalid age message
      throw( new InvalidDataException("Age is not in valid range (18-100): " + _age) )
    }
    
    // return age
    _age
    
  } catch {
    case e: InvalidDataException =>
      // dealing with invalid data exception
      println(e.getMessage())
      -1.0
    case e: NumberFormatException =>
      // dealing with number format exception        
      e.printStackTrace()
      -1.0
    case _: Exception =>
      // dealing with any other unknown errors       
      println("Error source not known")
      -1.0
  }
  finally {
    // do clean up
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Next, invoke the method with different values to test it.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Test scenario 1: Invalid Age Range

// COMMAND ----------

var age = validateAgeAdvanced("10")  
println( "Age is: " + age )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Test scenario 2: String Literal (Not a number)

// COMMAND ----------

// Calling with a string literal
var age = validateAgeAdvanced("as")  
println( "Age is: " + age )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Test scenario 3: Valid Age

// COMMAND ----------

// Calling with valid age
var age = validateAgeAdvanced("78")  
println( "Age is: " + age )

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 4: Using Try for Functional Exception Handling
// MAGIC 
// MAGIC Throwing an exception violates a fundamental principle of functional programming -- specifically, that functions should not have side effects. A pure function should have no external dependencies and produce a value of a defined type.
// MAGIC 
// MAGIC To handle exceptional conditions in a functional manner, Scala includes an abstract class called **`Try[A]`**, where **`A`** is the type of value returned if an exception does not occur. The **`Try`** class has two concrete subclasses:
// MAGIC 
// MAGIC * **`Success[A]`**, representing successful evaluation, which contains a value of type **`A`**
// MAGIC * **`Failure`**, representing failed evaluation, which can contain any type of **`Throwable`**
// MAGIC 
// MAGIC An idiomatic way to handle exceptional conditions in Scala is for a function to return **`Try[A]`**. You can then perform pattern matching on the result. Typical usage follows the pattern:
// MAGIC 
// MAGIC ```
// MAGIC   val ret: Try[Int] = method(parameters)
// MAGIC   val result = ret match {
// MAGIC     case Success(returnedValue) => returnedValue // Or do something extra
// MAGIC     case Failure(ex) => // handle the exception, ex, and return as appropriate
// MAGIC   }
// MAGIC 
// MAGIC ```
// MAGIC 
// MAGIC When invoking a method that can throw an exception (which is often the case for methods defined in Java), we can also take advantage of the syntax **`Try(expression)`**, which evaluates the given **`expression`** and returns a **`Success`** of the result if successful or a **`Failure`** if the expression throws an exception.

// COMMAND ----------

import scala.util.{Try, Success, Failure}

def validateAgeNew(age: String): Double = {
  Try(age.toDouble) match {
    case Success(_age) =>
      _age
    case Failure(ex) =>
      ex.printStackTrace()
      -1.0
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Invoking the methods defined above with valid input:

// COMMAND ----------

val goodAge = validateAgeNew("19")

// COMMAND ----------

// MAGIC %md
// MAGIC Invoking the methods defined above with invalid input:

// COMMAND ----------

val badAge = validateAgeNew("foo")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
