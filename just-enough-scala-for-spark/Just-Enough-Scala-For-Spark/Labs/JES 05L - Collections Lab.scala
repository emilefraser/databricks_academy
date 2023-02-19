// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Vectors

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Create a vector sequence of words 
// MAGIC 2. Apply vector operations like finding distinct words, reversing the vector, selecting the first few elements, etc.  

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// MAGIC %md
// MAGIC ##Exercise 1
// MAGIC 
// MAGIC Define a vector named `vectorOfWords` with these five words: `the`, `dog`, `visited`, `the`, and `firehouse`.

// COMMAND ----------

// TODO

// Define a vector named vectorOfWords and initilize with the list of words (The, dog, visited, the, firehouse)
FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var vectorOfWordsExpected = Vector("the", "dog", "visited", "the", "firehouse")
assert (vectorOfWords == vectorOfWordsExpected, s"Expected the result to be ${vectorOfWordsExpected} but found ${vectorOfWords}")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Get the unique elements of the above vector and assign the result to the variable `uniqueWordsVector`.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `distinct`.

// COMMAND ----------

// TODO

FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var uniqueWordsVectorExpected = Vector("the", "dog", "visited", "firehouse")
assert (uniqueWordsVector == uniqueWordsVectorExpected, s"Expected the result to be ${uniqueWordsVectorExpected} but found ${uniqueWordsVector}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Sort the words in the vector `uniqueWordsVector` and assign the result to the variable `sortedWordsVector`.

// COMMAND ----------

// TODO

FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var sortedWordsVectorExpected = Vector("dog", "firehouse", "the", "visited")
assert (sortedWordsVector == sortedWordsVectorExpected, s"""Expected the result to be ${sortedWordsVectorExpected} but found ${sortedWordsVector}""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 4
// MAGIC 
// MAGIC Reverse the words in the vector `sortedWordsVector` and assign the result to the variable `reversedWordsVector`. Then print the words in `reversedWordsVector` using a `for` loop.

// COMMAND ----------

// TODO

// Revese the words in the vector
FILL_IN 

// Print the words using a for loop
FILL_IN

// COMMAND ----------

// Test: Run this cell to test your solution.

var reversedWordsVectorExpected = Vector("visited", "the", "firehouse", "dog")
assert (reversedWordsVector == reversedWordsVectorExpected, s"Expected the result to be ${reversedWordsVectorExpected} but found ${reversedWordsVector}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Tuples

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC 1. Create a method named `weather` that takes two parameters named `temperature` and `humidity` both of type double. 
// MAGIC 2. The method should return a tuple of two values of type string.
// MAGIC   - The first element of the tuple should be `Hot` if the temperature is above 80 degrees and `Cold` if the temperature is below 50 degrees. Otherwise, return `Temperate`. 
// MAGIC   - The second element of the tuple should be `Humid` if the humidity is above 40%, unless the temperature is below 50; in that case, it should return `Damp`. Otherwise, return `Pleasant`.

// COMMAND ----------

// TODO

// Define the method weather
FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var expectedHeatMoistureTestOne = ("Hot", "Humid")
val heatMoistureTestOne = weather(90, 45)
assert (heatMoistureTestOne == expectedHeatMoistureTestOne, s"Expected the result to be ${expectedHeatMoistureTestOne} but found ${heatMoistureTestOne}")

var expectedHeatMoistureTestTwo = ("Cold", "Pleasant")
val heatMoistureTestTwo = weather(45, 30)
assert (heatMoistureTestTwo == expectedHeatMoistureTestTwo, s"Expected the result to be ${expectedHeatMoistureTestTwo} but found ${heatMoistureTestTwo}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC 1. Invoke the method `weather` with the temperature 100 and the humidity 60.
// MAGIC 2. Unpack the values of the returned tuple into the separate variables `heatOne` and `moistureOne`.

// COMMAND ----------

// TODO

// Invoke for Day One
FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var expectedheatOne = "Hot"
var expectedmoistureOne = "Humid"
assert (heatOne == expectedheatOne, s"Expected the result to be ${expectedheatOne} but found ${heatOne}")
assert (moistureOne == expectedmoistureOne, s"Expected the result to be ${expectedmoistureOne} but found ${moistureOne}")

// COMMAND ----------

// MAGIC %md
// MAGIC #Sets
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Create a set from three "baskets," each of which is a list of items, to find all the unique items present.
// MAGIC 2. Find the common items bought in two different baskets.
// MAGIC 3. Determine if one basket is a subset of another basket.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC 1. The items in three different baskets named `basket1`, `basket2`, and `basket3` are given as lists.
// MAGIC 2. Find the unique set of items bought across all the baskets and store the result in a varible `uniqueSetOfItems`. (Concatenate the lists, then convert the result to a set.)

// COMMAND ----------

// TODO

// Baskets and items in the baskets
val basket1 = List("beer", "milk", "potato", "egg")
val basket2 = List("beer", "potato", "egg")
val basket3 = List("milk", "potato", "carrot", "egg")

// Combine all the baskets in one basket
FILL_IN
// Find the unique set of items bought across all the baskets
val uniqueSetOfItems = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var uniqueSetOfItemsExpected = Set("potato", "milk", "egg", "carrot", "beer")
assert (uniqueSetOfItems == uniqueSetOfItemsExpected, s"Expected the result to be ${uniqueSetOfItemsExpected} but found ${uniqueSetOfItems}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Find the common set of items bought in `basket1` and `basket3`, and store the result in the variable `commonSetOfItems`.

// COMMAND ----------

// TODO

// Find the common set of items bought in basket1 and basket3
val commonSetOfItems = FILL_IN 

// COMMAND ----------

// Test: Run this cell to test your solution.

var commonSetOfItemsExpected = Set("milk", "potato", "egg")
assert (commonSetOfItems == commonSetOfItemsExpected, s"Expected the result to be ${commonSetOfItemsExpected} but found ${commonSetOfItems}")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Maps
// MAGIC 
// MAGIC In this lab exercise, you will...
// MAGIC 
// MAGIC 1. Create a map to store customer information as key-value pairs.
// MAGIC   - The key will be the email address, and other customer details like name and age will be stored as a tuple.
// MAGIC 2. Retrieve the customer information given an email address.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC 1. Define a *mutable* map named **`customerInfo`**
// MAGIC 2. Store the following customer information in the map. (The key should be the email id, and the value a tuple of the name and age.)
// MAGIC   - email id: **`sally@taylor.com`**
// MAGIC   - name: **`Sally Taylor`**
// MAGIC   - age: **`32`**

// COMMAND ----------

// TODO

// import mutable map
FILL_IN

// Initialize the map with the above customer information
var customerInfo = FILL_IN

// COMMAND ----------

// Test: Run this cell to test your solution.

var customerInfoExpected = ("Sally Taylor", 32)
assert (customerInfo("sally@taylor.com") == customerInfoExpected, s"Expected the result to be ${customerInfoExpected} but found ${customerInfo("sally@taylor.com")}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Add the following new customer information to the map:
// MAGIC   - email id: `jiminy@cricket.com`, name: `Jiminy Cricket`, age: `45`  

// COMMAND ----------

// TODO

// Add the new customer information
FILL_IN

// COMMAND ----------

// Test: Run this cell to test your solution.

var customerInfoExpected = ("Jiminy Cricket", 45)
assert (customerInfo("jiminy@cricket.com") == customerInfoExpected, s"Expected the result to be ${customerInfoExpected} but found ${customerInfo("sally@taylor.com")}")

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
