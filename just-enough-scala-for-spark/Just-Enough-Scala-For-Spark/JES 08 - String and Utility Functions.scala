// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC - A `String` literal can be delimited with double or triple-double quotes. 
// MAGIC - In Scala, as in Java, a `String` is an immutable object.

// COMMAND ----------

var s1 = "This is a one line string"

// Define a multiple line string using triple quotes
var s2 = """This is a multiple
            line string""" 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Applying String Methods
// MAGIC 
// MAGIC Scala uses the Java `String` class rather than defining its own. Therefore you can use all of the [standard Java `String` methods](https://docs.oracle.com/javase/10/docs/api/java/lang/String.html), such as:
// MAGIC 
// MAGIC - `length(): Int`                         : Returns the number of characters in the string
// MAGIC - `isEmpty(): Boolean`                    : Tests if this string has 0 characters
// MAGIC - `toLowerCase(): String`                 : Returns a new string that is a lower case version of the original string
// MAGIC - `toUpperCase(): String`                 : Returns a new string that is an upper case version of the original string
// MAGIC - `charAt(index: Int): Char`              : Returns the character at the specified index
// MAGIC - `substring(b1: Int, e1: Int): String`   : Returns a new string that is a substring of this string
// MAGIC - `trim(): String`                        : Returns a copy of the string, with leading and trailing whitespace omitted
// MAGIC - `replace(c1: Char, c2: Char): String`   : Returns a new string resulting by replacing all occurrences of `c1` in this string with `c2`
// MAGIC - `split(reg1: String): Array[String]`    : Splits this string around matches of the given regular expression
// MAGIC - `endsWith(suffix: String): Boolean`     : Tests if this string ends with the specified suffix
// MAGIC - `startsWith(prefix: String): Boolean`   : Tests if this string starts with the specified prefix
// MAGIC - `matches(regex: String): Boolean`       : Tests if this string matches the given regular expression
// MAGIC 
// MAGIC Strings can be concatenated using the `+` or `++=` operator.

// COMMAND ----------

// Getting a substring

val substr = s1.substring(0, 4)
println(substr)

// Convert to lower case

println(s1.toLowerCase())

// Splitting a string to tokens. Returns an Array of String

val tokens = s1.split(" ")

// String concatenation using + or ++= operator

var s3 = "First String"
var s4 = "Second String"

// s3 = s3 + s4

s3 ++= s4

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Formatting Strings

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Version 2.10 of Scala introduced support for *string interpolation*, which allows you to embed variable references or expressions directly in processed string literals. More details at: https://docs.scala-lang.org/overviews/core/string-interpolation.html
// MAGIC 
// MAGIC Prepending `s` to any string literal enables basic string interpolation. Prepending `f` to any string literal enables formatted string interpolation. 

// COMMAND ----------

val version = 2.4
println(s"This is spark version $version")

val a = 10
val b = 30

println( s"Sum of a and b is ${a + b}")


val platform = "Databricks Runtime"
val platformVersion = 5d  // A Double literal value

println(f"We are running $platform%s version $platformVersion%1.1f")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calling a method using infix notation
// MAGIC 
// MAGIC Scala *infix notation* is the ability to write a method call like
// MAGIC 
// MAGIC `a.method(b)`
// MAGIC 
// MAGIC in the text-like form as:
// MAGIC 
// MAGIC `a method b`
// MAGIC 
// MAGIC This feature is provided primarily to support the definition of custom operators in Scala. (Method names in Scala can contain operator characters like `+`, `-`, `*`, etc.) However, it can be used with any method.

// COMMAND ----------

// val char = "Hello".charAt(0) can be written as

val char = "Hello" charAt 0

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Utility Functions

// COMMAND ----------

// MAGIC %md
// MAGIC ### Useful Math Functions

// COMMAND ----------

// MAGIC %md
// MAGIC Scala math functions are defined in the [`scala.math` package](https://www.scala-lang.org/api/2.12.8/scala/math/index.html), such as `abs()`, `log()`, `pow()`, and `sqrt()`:

// COMMAND ----------

import scala.math.{abs, log, pow, sqrt}

val a = -2.0
println("The absolute value of a: " + abs(a))

val b = pow(a, 3)
println("The value of b: " + b)

println("The square root of 42: " + sqrt(42))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
