// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Comments, Variables, and Data Types
// MAGIC 
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC 
// MAGIC * Use comments to document your code
// MAGIC * Create and use variables
// MAGIC * Understand fundamental Scala data types

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Comments
// MAGIC 
// MAGIC Comments are non-executable text that document your code.
// MAGIC 
// MAGIC A single-line comment starts with **`//`** and continues to the end of the line:
// MAGIC 
// MAGIC <pre style="font-weight:bold">
// MAGIC // This entire line is a comment.
// MAGIC println("Hello, world!")  // All text from the first "//" to the end of the line is a comment.
// MAGIC </pre>
// MAGIC 
// MAGIC Multiline comments start with **`/*`** and end with **`*/`**:
// MAGIC 
// MAGIC <pre style="font-weight:bold">
// MAGIC /* This is the beginning of the comment.
// MAGIC  * The "*" at the beginning of this line isn't required, but provides a visual
// MAGIC  * indicator that the text is part of a multiline comment.
// MAGIC  */
// MAGIC </pre>
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If **`//`**, **`/*`**, or **`*/`** appear within a quoted text string, they are treated as literal characters and not comment delimiters.

// COMMAND ----------

// This is a comment.
println("These are literal // characters.")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Values and Variables
// MAGIC 
// MAGIC - Named values can be created with the keyword **`val`** and assigned a constant value. Values are **immutable**.
// MAGIC - Variables can be created using the **`var`** keyword. Variables are **mutable**.
// MAGIC - The name of value or variable can begin with a letter or underscore.
// MAGIC - Names are case sensitive.
// MAGIC 
// MAGIC Coding standard guidelines: Use camelCase names for named values and variables.

// COMMAND ----------

val sparkVersion = 2.4

// COMMAND ----------

var noOfParticipants = 20
println(noOfParticipants)

noOfParticipants = 30  // Changes the value assigned to the variable
println(noOfParticipants)

println("-" * 80)  // Prints a visual delimiter of 80 "-" characters

// COMMAND ----------

// MAGIC %md
// MAGIC The compiler reports an error (**`error: reassignment to val`**) if there is any attempt to change a named value.

// COMMAND ----------

// The following line results in a compilation error if you uncomment it and run this cell.
// sparkVersion = 2.5

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> A feature of the Scala REPL -- but *not* of compiled Scala code -- is the ability to re-declare an existing variable or named value. You should avoid doing this to prevent confusion, especially in production code.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Data Types

// COMMAND ----------

// MAGIC %md
// MAGIC All Scala types are defined as _classes_ or _traits_ (which are similar to Java _interfaces_). Common Scala types include:
// MAGIC 
// MAGIC - **`Byte`**: 8-bit signed value. Range from -128 to 127
// MAGIC - **`Short`**: 16-bit signed value. Range -32768 to 32767
// MAGIC - **`Int`**: 32-bit signed value. Range -2147483648 to 2147483647
// MAGIC - **`Long`**: 64-bit signed value. Range -9223372036854775808 to 9223372036854775807
// MAGIC - **`Float`**: 32-bit IEEE 754 single-precision float
// MAGIC - **`Double`**: 64-bit IEEE 754 double-precision float
// MAGIC - **`Char`**: 16-bit unsigned Unicode character. Range U+0000 to U+FFFF
// MAGIC - **`Boolean`**: Either the literal **`true`** or the literal **`false`**
// MAGIC - **`Unit`**: A type consisting of only the *unit value*, represented by **`()`**, used to indicate the return value of a function that performs a side effect -- such as **`println()`**. (Any Java functions with a **`void`** return type are treated as having a **`Unit`** return type when called from Scala.)
// MAGIC - **`Any`**: The supertype of all types. Any object is of type **`Any`**
// MAGIC 
// MAGIC Scala can also use types defined by Java classes and interfaces. A common example is **`String`**.
// MAGIC 
// MAGIC For more information about the Scala type hierarchy, see <a href="https://docs.scala-lang.org/tour/unified-types.html" target="_blank">https&#58;//docs.scala-lang.org/tour/unified-types.html</a>
// MAGIC 
// MAGIC The Scala compiler can automatically **infer** the data type of a variable based on the type of value initially assigned to it. You can also explicitly declare a variable's type like this:

// COMMAND ----------

val pi: Double = 3.14159265358979

// COMMAND ----------

// MAGIC %md
// MAGIC To find the data type of a value or variable, call its `getClass()` method.

// COMMAND ----------

sparkVersion.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Expressions
// MAGIC Nearly everything in Scala is an *expression*, which is code evaluated to produce a value.
// MAGIC 
// MAGIC - Examples of simple expressions are arithmetic expressions, function calls, and conditional expressions.
// MAGIC - Block expressions consist of one or more expressions surrounded by curly braces. The expressions are evaluated in sequence, and the result of the last expression evaluated is the result of the block expression.
// MAGIC - A block expression can contain any number of other expressions, including other curly-braced block expressions.
// MAGIC 
// MAGIC Coding guidelines: 
// MAGIC - Use 2-space indentation for the expressions within a block.
// MAGIC - Put one space before and after operators, including the assignment operator.

// COMMAND ----------

val comp = {
  val size = 2         // A value accessible only within this block
  val mark = 4 / size  // Another value accessible only within this block
  size * mark          // Final value of the expression
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
