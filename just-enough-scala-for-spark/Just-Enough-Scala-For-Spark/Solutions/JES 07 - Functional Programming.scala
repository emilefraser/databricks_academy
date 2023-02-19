// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

val listOfNumbers = Range(1, 11) // Create a sequence of integers

// Create an empty buffer
var squareOfNumbers = new scala.collection.mutable.ArrayBuffer[Int]()

// Iterate or loop through the sequence and apply transformation (here, calculating square)
for ( i <- listOfNumbers ) {
  squareOfNumbers += i * i
}

println("Squares: " + squareOfNumbers)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The above code can be written using a functional `map()` operation, which is less verbose, more readable, and can be parallelized.

// COMMAND ----------

// This method returns square value of the parameter passed to it
def getSquare(x: Int) = {
  x * x
}


val squareOfNumbers = listOfNumbers.map(getSquare) 

// The function definition and mapping the elements can be written using an anonymous function as below

// val squareOfNumbers = listOfNumbers.map( x => x * x )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Example 2: Applying filter
// MAGIC 
// MAGIC The `filter()` method can be applied to iterate through a collection and return a new collection of those elements that satisfy the condition specified.
// MAGIC 
// MAGIC For example, to filter out the odd integers and retain only the even numbers:
// MAGIC - Use a filter that returns true if the number is even and false if not even
// MAGIC - This can be done using an inline condition expression or named function

// COMMAND ----------

// Using inline condition 
val evenNumbers = listOfNumbers.filter( x => x % 2 == 0 )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Using a Named Function

// COMMAND ----------

def isEven(x: Int) = { x % 2 == 0 }

// Using the named function in filter()

val evenNumbersList = listOfNumbers.filter(isEven)

// Multiple ways of calling the function
// val evenNumbersList = listOfNumbers.filter(x => isEven(x))
// val evenNumbersList = listOfNumbers.filter(isEven _)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Example 3: Applying reduce
// MAGIC 
// MAGIC The `reduce()` method can be used to collapse all elements of a collection, like summing up all elements in a collection.
// MAGIC 
// MAGIC For example, summing up all elements can be done as follows:
// MAGIC 
// MAGIC 1. Sum up the first two elements
// MAGIC 2. Add the third element to the above sum
// MAGIC 3. Add the fourth element to the above sum
// MAGIC 4. Keep on adding the elements with the last sum until all the elements are exhausted.
// MAGIC 
// MAGIC This is called a *reduce left* operation.
// MAGIC 
// MAGIC This requires defining a function that takes two parameters and returns the sum of those two values. 

// COMMAND ----------

// MAGIC %md
// MAGIC In Scala the operation can be written as:
// MAGIC 
// MAGIC <code>
// MAGIC listOfNumbers.reduceLeft((a: Int, b:Int) => a + b)
// MAGIC </code>  
// MAGIC 
// MAGIC Alternatively, when declaring an anonymous function in Scala, we can use `_` as a *positional parameter*. The occurrences of `_` in the function definition get replaced by the corresponding arguments passed. (This implies that the value of each argument can be referred to only once in the function definition.) So the same operation as above could be written as:
// MAGIC 
// MAGIC <code>
// MAGIC listOfNumbers.reduceLeft(\_ + \_)
// MAGIC </code> 

// COMMAND ----------

// listOfNumbers.reduce((a, b) => a + b)

listOfNumbers.reduceLeft(_ + _)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
