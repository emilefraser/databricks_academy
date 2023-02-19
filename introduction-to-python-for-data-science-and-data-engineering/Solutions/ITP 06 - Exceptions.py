# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="98014313-352a-4426-b032-c0c48f96180a"/>
# MAGIC 
# MAGIC 
# MAGIC # Errors and Exceptions
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC 
# MAGIC - Explore errors and exceptions
# MAGIC - Review assert statements
# MAGIC - Employ try-catch for exception handling

# COMMAND ----------

# MAGIC %md <i18n value="069b8442-7d9f-4418-8bc2-6426248ee458"/>
# MAGIC 
# MAGIC 
# MAGIC ### Syntax Errors
# MAGIC 
# MAGIC In Python, there are mainly two different kinds of errors: Syntax Errors and Exceptions. Syntax Errors are errors that are thrown because code was typed incorrectly and Python does not know how to interpret it. 
# MAGIC 
# MAGIC The example below illustrates a Syntax Error.

# COMMAND ----------

# if print("Hello World") # This is not correct Python code, so it throws a Syntax Error

# COMMAND ----------

# MAGIC %md <i18n value="6fdcf2ae-e9d7-4686-81d7-eec2c9141af1"/>
# MAGIC 
# MAGIC 
# MAGIC ### Exceptions
# MAGIC 
# MAGIC If we have properly formatted code that Python knows how to run, we might still encounter errors as the code is executed. Errors that arise like this as the code is executed are known as Exceptions. They indicate that, while Python understood what we were trying to do, there is a problem.
# MAGIC 
# MAGIC The example below illustrates an Exception.

# COMMAND ----------

#  1 / 0 

# COMMAND ----------

# MAGIC %md <i18n value="7dea8495-1d7e-448b-bd22-16f243bc753c"/>
# MAGIC 
# MAGIC 
# MAGIC This time we observed a **`ZeroDivisionError`** exception, indicating that we tried to divide by zero, which is not defined. There are different exceptions provided by Python that indicate different problems, a full list of the built-in ones can be found [here](https://docs.python.org/3/library/exceptions.html#bltin-exceptions).

# COMMAND ----------

# MAGIC %md <i18n value="215acf78-5049-40ff-b7b5-1050a889b315"/>
# MAGIC 
# MAGIC 
# MAGIC ### Exception Handling
# MAGIC 
# MAGIC Syntax Errors will always cause our programs to exit and fail, but we can handle exceptions in Python. This allows us to program what happens in the case that Python encounters an exception or specific exceptions when trying to run a code block. To handle exceptions in Python we use a **`try`** statement. 
# MAGIC 
# MAGIC We write a **`try`** statement like this:
# MAGIC 
# MAGIC ```
# MAGIC try:
# MAGIC     code block
# MAGIC except:
# MAGIC     code block
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC When Python encounters a **`try`** statement, it will first try to run the code in the **`try`** code block. If it encounters an exception, instead of exiting and erroring like we have seen, it will instead run the code block under **`except`**.

# COMMAND ----------

try:
    1/0 # Throws an exception
except:
    print("Exception Handled")

# COMMAND ----------

# MAGIC %md <i18n value="daba1041-3381-4a20-9aa5-ee991c67ce4d"/>
# MAGIC 
# MAGIC 
# MAGIC In this last example, we run the **`except`** block if we encounter any exception in the **`try`** block. If we want to only handle a certain exception we can write the exception after the **`except`** keyword like this:
# MAGIC 
# MAGIC ```
# MAGIC try:
# MAGIC     code block
# MAGIC except ExceptionName:
# MAGIC     code block
# MAGIC 
# MAGIC ```

# COMMAND ----------

try:
    1/0 # Throws a ZeroDivisionError exception
except ZeroDivisionError:
    print("Exception Handled")

# COMMAND ----------

# try:
#     print(undefined_variable) # Throws a Name Error exception
# except ZeroDivisionError:
#     print("Exception Handled")

# COMMAND ----------

# MAGIC %md <i18n value="9ae04843-cd61-4ba7-9136-6c7c04717982"/>
# MAGIC 
# MAGIC 
# MAGIC If we want to handle multiple specific exceptions we can write a sequence of exceptions separated by commas inside parentheses. 
# MAGIC 
# MAGIC Try commenting out one of the exception throwing lines below at a time, and notice that both exceptions are handled.

# COMMAND ----------

try:
    1/0 # Throws a ZeroDivisionError exception
    print(undefined_variable) # Throws a Name Error exception
except (ZeroDivisionError, NameError):
    print("Exception Handled")

# COMMAND ----------

# MAGIC %md <i18n value="0ce9ca24-9951-488d-970e-8acbeb86b750"/>
# MAGIC 
# MAGIC 
# MAGIC This now handles both the ZeroDivisionError and NameError exceptions.

# COMMAND ----------

# MAGIC %md <i18n value="2cbcc8d4-fc13-434f-a1df-1b82a9f7a46c"/>
# MAGIC 
# MAGIC 
# MAGIC ### Assertion Error
# MAGIC 
# MAGIC One very useful exception is called an **`AssertionError`**. We can raise **`AssertionErrors`** using **`assert`** statements. We have been using these to check your lab code during this course. We use an **`assert`** statement like this:
# MAGIC 
# MAGIC ```
# MAGIC assert boolean expression, optional message
# MAGIC ```
# MAGIC 
# MAGIC When Python executes this statement, it evaluates the boolean expression first. If it is **`True`**, Python does nothing and moves on. If it is **`False`**, then it throws an AssertionError with the optional message, if provided.

# COMMAND ----------

# assert 1 == 1
# assert 1 == 2, "That is not true"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
