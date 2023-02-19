// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classes

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * Classes can be declared using the **`class`** keyword.
// MAGIC * Class methods are declared with the **`def`** keyword.

// COMMAND ----------

// MAGIC %md
// MAGIC * Methods and fields are public by default, but can be specified as **`protected`** or **`private`**.
// MAGIC   * Scala does not have an explicit **`public`** keyword.
// MAGIC * Constructor arguments are by default **`private`**, but can be proceeded by **`val`** to be made public.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Example 1
// MAGIC 
// MAGIC * Create a customer class called **`Customer`**
// MAGIC * It should have three attributes:
// MAGIC   * **`firstName`**
// MAGIC   * **`lastName`**
// MAGIC   * **`value`**
// MAGIC * Add a method that combines the first & last name in the form of "Last, First"
// MAGIC   * Name the method **`fullName`**

// COMMAND ----------

class Customer(val firstName: String, 
               val lastName: String, 
               val value: Int) {
  
  def fullName():String = {
    lastName + ", " + firstName
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To create an instance of the class:

// COMMAND ----------

val customer = new Customer("Thomas", "Jefferson", 195)

println("First Name: " + customer.firstName )
println("last Name:  " + customer.lastName )
println("Value:      " + customer.value )
println("Full Name:  " + customer.fullName() )
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Case Classes
// MAGIC 
// MAGIC - Case classes are like <a href="https://en.wikipedia.org/wiki/Plain_old_Java_object" target="_blank">POJO</a> classes
// MAGIC - Their intended use is as "struct" type classes, which can have getter and setter methods to access member variables
// MAGIC - All constructor parameters are treated automatically as immutable public instance fields
// MAGIC - The compiler automatically generates lots of "boilerplate" code -- for example, `equals()` and `toString()` methods
// MAGIC - Case classes are good for modeling immutable data -- for example, representing records in a dataset

// COMMAND ----------

case class Employee(name: String, age: Integer, salary: Double, role: String)

val firstEmp = Employee("R Sharma", 30, 123000, "Manager")

println("Name:   %s".format(firstEmp.name))
println("Age:    %s".format(firstEmp.age))
println("Salary: %s".format(firstEmp.salary))
println("Role:   %s".format(firstEmp.role))
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Putting Together Case Classes and Collections
// MAGIC 
// MAGIC - Define a set of Employee objects using the case class
// MAGIC - Store them in a Vector collection
// MAGIC - Use a for loop to access properties of each instance

// COMMAND ----------

val employeeList = Vector( Employee("R Sharma", 30, 123000, "Manager"),
                           Employee("G Adams", 30, 78000, "Supervisor"),
                           Employee("S Peter", 30, 145000, "Manager"),
                           Employee("H Xia", 30, 89000, "Engineer"),
                           Employee("M Anand", 30, 110000, "Engineer"),
                           Employee("J Norvic", 30, 99000, "Supervisor"))

// COMMAND ----------

val numOfEmployees =  employeeList.size

println("Total Number of employees: " + numOfEmployees)

for (employee <- employeeList) {
  println ((employee.name + " - " + employee.role))
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
