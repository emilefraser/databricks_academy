# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="4db476c9-19cb-4f89-ba8b-105c88ebcdea"/>
# MAGIC 
# MAGIC 
# MAGIC # Control Flow
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC 
# MAGIC Apply basic control flow concepts in Python we learned last lesson, including:
# MAGIC 
# MAGIC * **`if`** and **`elif`** statements
# MAGIC * More boolean operators
# MAGIC * Type checking

# COMMAND ----------

# MAGIC %md <i18n value="4d276a26-f723-4ccc-bfe5-3ad2c0b14f90"/>
# MAGIC 
# MAGIC 
# MAGIC ### Food Recommender
# MAGIC 
# MAGIC For this lab, write the control flow logic for the following food recommender. Users provide the following input:
# MAGIC 
# MAGIC *  **`temperature`**: A float representing the temperature outside (in Fahrenheit)
# MAGIC *  **`sunny`**: A boolean set to **`True`** if it is sunny outside, **`False`** otherwise. 
# MAGIC 
# MAGIC Write the system to print the following recommendations:
# MAGIC 
# MAGIC * If it is at least 60 degrees outside AND it is sunny, recommend `ice cream`.
# MAGIC * If it is at least 60 degrees outside, but it is not sunny, recommend `dumplings`.
# MAGIC * If if it is less than 60 degrees, regardless of the weather, recommend `hot tea`.

# COMMAND ----------

# ANSWER
temperature = 72.0
sunny = False

if temperature >= 60.0 and sunny:
    print("ice cream")
elif temperature >= 60.0 and not sunny:
    print("dumplings")
else:
    print("hot tea")

# COMMAND ----------

# MAGIC %md <i18n value="7f61fbe1-880d-450f-b1df-26c7450378cc"/>
# MAGIC 
# MAGIC 
# MAGIC Try changing the values of **`temperature`** and **`sunny`** and make sure it recommends the proper foods!

# COMMAND ----------

# MAGIC %md <i18n value="9ff5549b-cf14-4943-b89d-fd65ee6a260a"/>
# MAGIC 
# MAGIC 
# MAGIC ## Bonus Exercise 
# MAGIC 
# MAGIC For this part of the lab, write the control flow logic for car maintenance. Users provide the following input:
# MAGIC 
# MAGIC *  **`km_since_last_change`**: An integer representing the number of kilometers since the last oil change
# MAGIC *  **`oil_change_light`**: A boolean set to **`True`** if the oil change light is on, **`False`** otherwise. 
# MAGIC 
# MAGIC Write the system to print the following recommendations:
# MAGIC 
# MAGIC * If it is at least 15000 kilometers since last oil change AND the light is on, recommend `oil change`.
# MAGIC * If it is at least 15000 kilometers since last oil change AND the light is off, recommend `wait`.
# MAGIC * If it is less than 15000 kilometers since last oil change regardless of the light, recommend `wait`.

# COMMAND ----------

# ANSWER
km_since_last_change = 15000
oil_change_light = True

if km_since_last_change >= 15000 and oil_change_light :
    print("Time for an oil change")
elif km_since_last_change >= 15000 and not oil_change_light:
    print("Wait for the light")
else:
    print("Wait longer")

# COMMAND ----------

# MAGIC %md <i18n value="5cbae134-c726-4874-b416-a2b802236a6f"/>
# MAGIC 
# MAGIC ### Bonus Exercise
# MAGIC 
# MAGIC A year is considered a leap year if it.
# MAGIC - Is evenly divisible by 4 AND ...
# MAGIC   - Is either evenly divisible by 400 (e.g. 2000 was a leap year) OR not evenly divisible by 100 (e.g. 2100 will not be a leap year).
# MAGIC - How do you know if a number is evenly divisible by another number?
# MAGIC   - Modulo division
# MAGIC     - 2000 % 4 == 0: True
# MAGIC     - 1901 % 4 == 0: False
# MAGIC   
# MAGIC Write code to implement the logic described above.
# MAGIC - Name the independent variable __year__.
# MAGIC - Name the dependent variable named __is_leap_year__.
# MAGIC - Test the code for the following years:
# MAGIC   - 1900
# MAGIC   - 1901
# MAGIC   - 1904
# MAGIC   - 2000
# MAGIC 
# MAGIC **Hint**: The nested indentation in the instruction suggests that nested logic may be appropriate here.

# COMMAND ----------

dbutils.widgets.text("year", "2022", "Enter Year Here")

# COMMAND ----------

# ANSWER
year = int(dbutils.widgets.get("year"))
is_leap_year = False
if year % 4 == 0:
    if year % 400 == 0 or year % 100 != 0:
        is_leap_year = True

# COMMAND ----------

# Check your work

if year == 1900:
    assert year == 1900 and is_leap_year == False, "Error: 1900 was not a leap year"
elif year == 1901:
    assert year == 1901 and is_leap_year == False, "Error: 1901 was not a leap year"
elif year == 1904:
    assert year == 1904 and is_leap_year == True, "Error: 1904 was a leap year"
elif year == 2000:
    assert year == 2000 and is_leap_year == True, "Error: 2000 was a leap year"

# COMMAND ----------

dbutils.widgets.remove("year")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
