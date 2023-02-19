# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="1818ed45-df1d-4a39-8074-e2841494477c"/>
# MAGIC 
# MAGIC 
# MAGIC # Data Types and Variables Lab
# MAGIC 
# MAGIC ## Exercise:
# MAGIC * Define a variable **`num_chocolate`** and set it equal to the number of chocolate bars you would like to eat. These should be whole numbers (e.g. integer).
# MAGIC * Define a variable **`name`** and set it equal to your name.
# MAGIC * Define the string **`chocolate_string`** as follows: "**`name`** would like to eat **`num_chocolate`** bars of chocolate" using f-string formatting
# MAGIC * Print the string  **`chocolate_string`**

# COMMAND ----------

# ANSWER
name = "James"
num_chocolate = 10
chocolate_string = f"{name} would like to eat {num_chocolate} bars of chocolate"

print(chocolate_string)

# COMMAND ----------

# MAGIC %md <i18n value="d156910a-4562-4a24-bc6e-2d3ed9826f24"/>
# MAGIC 
# MAGIC 
# MAGIC **Check your work:**
# MAGIC 
# MAGIC For labs, we will have **Check your work** cells like the following after most questions. After you complete a question, run the cell to make sure you did it correctly. 
# MAGIC 
# MAGIC You do not have to know how to use the **assert** statements we use below. 
# MAGIC 
# MAGIC If you are curious, if the boolean expression after the assert statement does not evaluate to True, then the code stops and raises an error.

# COMMAND ----------

assert type(name) == str, "Name should be a string"
assert type(num_chocolate) == int, "You have to eat the entire chocolate bar! No floats allowed"
assert chocolate_string == f"{name} would like to eat {num_chocolate} bars of chocolate", "Did you mistype something?"
print("Test passed!")

# COMMAND ----------

# MAGIC %md <i18n value="TBD"/>
# MAGIC 
# MAGIC ## Bonus Exercise:
# MAGIC * Define a variable **`num_students`** and set it equal to the number of students in the class.
# MAGIC * Define a variable **`num_days`** and set it equal to the number of class days.
# MAGIC * Define a variable **`class_name`** and set it equal to the name of this class.
# MAGIC 
# MAGIC * Define the string **`class_information`**  as follows: "There are **`num_students`** * **`num_days`** student days in the **`class_name`**" using f-string formatting.  Make sure to print the product of students and days.
# MAGIC * Print the string  **`class_information`**

# COMMAND ----------

# ANSWER
num_students = 16
num_days = 2
class_name = "introduction-to-python-for-data-science-and-data-engineering"
course_information = f"There are {num_students * num_days} student days in the {class_name}"

print(course_information)

# COMMAND ----------

assert type(class_name) == str, "Name should be a string"
assert type(num_students) == int, "Use an integer"
assert course_information == f"There are {num_students * num_days} student days in the {class_name}"

print("Test passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
