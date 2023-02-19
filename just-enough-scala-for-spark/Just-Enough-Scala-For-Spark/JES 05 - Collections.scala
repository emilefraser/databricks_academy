// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Collections

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Scala provides mutable and immutable collections. 
// MAGIC - A mutable collection can be updated or extended in place. This means you can change, add, or remove elements of a collection. 
// MAGIC - Immutable collections, by contrast, never change. So, any operation like additions, removals, or updates, returns a new collection and leave the old collection unchanged.
// MAGIC 
// MAGIC Scala supports the following collection types, defined by **traits** (equivalent to Java **interfaces**):
// MAGIC 
// MAGIC - `Seq`, an ordered sequence of elements
// MAGIC - `Set`, a collection of unique elements
// MAGIC - `Map`, a collection of key-value pairs
// MAGIC 
// MAGIC There are several concrete classes, mutable and immutable, implementing each of the collection traits. The different implementations have different performance characterists for various operations. For more information, see https://docs.scala-lang.org/overviews/collections/performance-characteristics.html
// MAGIC 
// MAGIC Scala collection classes and traits are located in the packages `scala.collection`, `scala.collection.immutable`, and `scala.collection.mutable`. 
// MAGIC 
// MAGIC Scala also supports **tuples**, which are simple structures that can hold elements of different types.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Seq (Sequence)
// MAGIC 
// MAGIC - Sequences are ordered collections of objects
// MAGIC - Sequences support random access with 0-based indexing
// MAGIC - Common operations like appending, inserting, removing, and updating elements return a new sequence
// MAGIC - Mutable sequence implentations also support various methods for in-place operations that modify the sequence
// MAGIC 
// MAGIC #### Vector
// MAGIC 
// MAGIC - `Vector` is an immutable implementation of `Seq`
// MAGIC - It is available in the package `scala.collection.immutable` as `scala.collection.immutable.Vector`
// MAGIC - Vectors are optimized for random access of elements

// COMMAND ----------

val v1 = Vector(1, 3, 5, 7, 11, 13, 9)

// COMMAND ----------

// Accessing the fourth element
val index3Value = v1(3)

println("Fourth element in the vector is: " + index3Value )

// Checking if an element is available in the collection.
val isAvailable = v1.contains(5)

println( "If value 5 is available in the vector: " + isAvailable)

// COMMAND ----------

// Examples of common Seq operations

println( "-" * 80 )

println("Original vector (v1): " + v1)
println( "-" * 80 )

// Appending an element to the vector using ':+' operator
val v2 = v1 :+ 4

println("After appending an element to the vector (v2): " + v2)
println( "-" * 80 )

// Prepending an element to the vector using '+:' operator
val v3 = 0 +: v2

println("After prepending an element to the vector (v3): " + v3)
println( "-" * 80 )

// Sorting a vector
val v4 = v3.sorted

println("After sorting the vector (v4): " + v4)
println( "-" * 80 )

// Appending a vector to an existing vector
var v5 = v4 ++ Vector(15, 20)

println("After appending a new vector to the existing vector (v5): " + v5)
println( "-" * 80 )

// Reversing a vector
val v6 = v5.reverse

println("After reversing the vector (v6): " + v6)
println( "-" * 80 )

// Return the first element
val vHead = v6.head

println("First element (v6): " + vHead)
println( "-" * 80 )

// Return all elements after the head
val vTail = v6.tail

println("Tail elements (v6): " + vTail)
println( "-" * 80 )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Tuple

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - A tuple is a structure that can hold elements of different types. 
// MAGIC - Tuples are immutable.

// COMMAND ----------

// MAGIC %md
// MAGIC * The tuple index start with 1, not 0 in Scala
// MAGIC * Scala tuples are limited to a maximum of 22 elements

// COMMAND ----------

val constantVal = ("pi", 3.14)

// COMMAND ----------

// unpacking a tuple individual variable elements 
val (constant, value) = constantVal

println("Constant name: " + constant + " and it's value: " + value)

// Accessing elements in tuple by index
println( constantVal._1 )
println( constantVal._2 )

// COMMAND ----------

// MAGIC %md
// MAGIC Scala also supports a special syntax for creating a 2-element tuple:

// COMMAND ----------

val texCap = "TX" -> "Austin"

println( texCap._1 )
println( texCap._2 )

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Tuples come in handy when we have to return multiple values from a function.

// COMMAND ----------

// Returning multiple values from a method 
// takes a vector of double values as input and returns minimum and maximum value
def getMinMax(input: Vector[Double]): (Double, Double) = {
  val sortedInput = input.sorted
  (sortedInput(0), sortedInput(input.size - 1))
}

//  invoking the method
val ages = Vector(52.0, 34.0, 12.0, 76.0, 28.9)
val minMaxAge = getMinMax(ages)

// printing the results
println("Minimum Age:" + minMaxAge._1)
println("Maximum Age:" + minMaxAge._2)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Set
// MAGIC 
// MAGIC - A set ensures that it contains only one element of each value, so it automatically removes duplicates
// MAGIC - Supports set operations like union, intersection, difference, etc. 

// COMMAND ----------

val firstSet = Set(1, 1, 2, 3, 9, 9, 4, 22, 11, 7, 6)
val secondSet = Set(2, 3, 4, 99)

// COMMAND ----------

// Union of two sets
var unionOfSets = firstSet.union(secondSet)

println("Union of sets: " + unionOfSets)

// difference of two sets can be computed using "--" operator
var diffSets = firstSet -- secondSet
println("Difference of sets: " + diffSets)

// Intersection of two sets
var intersecSets = firstSet.intersect(secondSet)
println("Intersection of sets: " + intersecSets)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Map
// MAGIC 
// MAGIC - A `Map` is like a dictionary, associating keys to values
// MAGIC - The keys in a `Map` are unique

// COMMAND ----------

var constants = Map("Pi" -> 3.14,
                    "e" -> 2.718, 
                    "phi" -> 1.618)

// Lookup of values from the map

var eValue = constants("e")

println("Value of e is: " + eValue)

// Get all keys

println("List of keys: " + constants.keys)

// Get all values

println("List of values: " + constants.values)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Sequence Comprehensions
// MAGIC 
// MAGIC Sequence comprehensions are used to create a new list by applying an algorithm to each value in an existing collection. 

// COMMAND ----------

// Use comprehension to apply methods to each string in a collection
val nameVec = Vector("ahir", "quinn", "vini", "sue", "amanda" )
// for each value in the original list, capitalize the first letter
val capNameVec = for (e <- nameVec) yield e.capitalize


// Use comprehensions to perform mathmatic operations on each element in a collection
val priceVec = Vector(1, 5, 10, 20)
// for each value in the original price list, increasae by 15%
val newPriceVec = for (e <- priceVec) yield e * 1.15

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
