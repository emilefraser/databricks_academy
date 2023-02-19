// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Getting Started with Scala
// MAGIC 
// MAGIC ## In this lesson you'll learn:
// MAGIC 
// MAGIC * Key Scala Internet resources
// MAGIC * How to compile and run Scala code in various environments

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) What is Scala?
// MAGIC 
// MAGIC > Scala is a modern multi-paradigm programming language designed to express common programming patterns in a concise, elegant, and type-safe way.
// MAGIC > It smoothly integrates features of object-oriented and functional languages.
// MAGIC 
// MAGIC 
// MAGIC <a href="https://docs.scala-lang.org/tour/tour-of-scala.html" target="_blank">https&#58;//docs.scala-lang.org/tour/tour-of-scala.html</a>
// MAGIC 
// MAGIC Key Points:
// MAGIC * Scala code compiles to Java bytecode that can run on any Java Virtual Machine (JVM)
// MAGIC * Scala supports object-oriented and functional programming
// MAGIC * Scala is statically typed -- that is, the compiler performs data type checking (unlike dynamically typed languages like Python)
// MAGIC - Created by Martin Odersky and first released in 2004

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Important Links
// MAGIC 
// MAGIC Links:
// MAGIC * The Scala home page is at: <a href="https://www.scala-lang.org/" target="_blank">https&#58;//www.scala-lang.org</a>
// MAGIC * Scala documentation is available at: <a href="https://docs.scala-lang.org/" target="_blank">https&#58;//docs.scala-lang.org</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Compiling and Running Scala Programs
// MAGIC 
// MAGIC This is a Scala program that simply prints "Hello, world!" to the terminal when executed:
// MAGIC 
// MAGIC <pre style="font-weight:bold">
// MAGIC object HelloWorld {
// MAGIC   def main(args: Array[String]): Unit = {
// MAGIC     println("Hello, world!")
// MAGIC   }
// MAGIC }
// MAGIC </pre>
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By convention, Scala source files use a **`.scala`** extension.
// MAGIC 
// MAGIC * Use the **`scalac`** compiler to compile Scala source code into Java bytecode (Java **`.class`** files):
// MAGIC 
// MAGIC   <pre style="font-weight:bold">$ scalac HelloWorld.scala</pre>
// MAGIC 
// MAGIC * Run your compiled program with the **`scala`** command:
// MAGIC 
// MAGIC   <pre>$ scala –classpath . HelloWorld</pre>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Scala REPL
// MAGIC 
// MAGIC Scala also includes a REPL (“read-eval-print-loop”) that functions as an interpreter for Scala code. Using the REPL you can interactively execute and test out Scala code.
// MAGIC 
// MAGIC You start the REPL from the terminal by executing the **`scala`** command with no arguments. For example:
// MAGIC 
// MAGIC <pre style="font-weight:bold">
// MAGIC $ scala
// MAGIC Welcome to Scala 2.12.8 (OpenJDK 64-Bit Server VM, Java 11.0.1).
// MAGIC Type in expressions for evaluation. Or try :help.
// MAGIC 
// MAGIC scala> println("Welcome to Databricks Academy")
// MAGIC Welcome to Databricks Academy
// MAGIC 
// MAGIC scala> :quit
// MAGIC $ 
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Scala and IDEs
// MAGIC 
// MAGIC For developing Scala in an integrated development environment (IDE), there are two main choices:
// MAGIC 
// MAGIC * IntelliJ IDEA: <a href="https://www.jetbrains.com/idea" target="_blank">https&#58;//www.jetbrains.com/idea</a> (using the Scala plugin)
// MAGIC * The Scala IDE: <a href="http://scala-ide.org" target="_blank">http&#58;//scala-ide.org</a> (based on the Eclipse IDE)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Notebook Interfaces for Scala Development
// MAGIC 
// MAGIC Databricks provides a notebook interface for developing executable code, visualizations, and narrative text. When you run an executable code cell in a notebook, the code is executed in a Scala REPL on the Databricks cloud platform and the results are displayed in the notebook.
// MAGIC 
// MAGIC Other notebook interfaces with support for Scala include:
// MAGIC 
// MAGIC * Apache Zeppelin: <a href="https://zeppelin.apache.org" target="_blank">https&#58;//zeppelin.apache.org</a>
// MAGIC * Jupyter Notebook: <a href="https://jupyter.org" target="_blank">https&#58;//jupyter.org</a>
// MAGIC 
// MAGIC Try executing the following code cell.

// COMMAND ----------

println("Hello, world!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
