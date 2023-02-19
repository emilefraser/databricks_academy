# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="65df298d-79ae-4ce6-9e3a-b736cde03178"/>
# MAGIC 
# MAGIC 
# MAGIC # Functions
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Define and use functions to reuse code
# MAGIC   * Arguments
# MAGIC   * Type hints
# MAGIC * Invoke the **`help()`** function 

# COMMAND ----------

# MAGIC %md <i18n value="568cdf5f-5b84-4bf8-8709-4b58765d2640"/>
# MAGIC 
# MAGIC 
# MAGIC ## Functions
# MAGIC 
# MAGIC In this lesson, we're going to see how we can use <a href="https://www.w3schools.com/python/python_functions.asp" target="_blank">functions</a> to make code reusable.
# MAGIC 
# MAGIC We define a function like this:
# MAGIC 
# MAGIC       def function_name(parameters):
# MAGIC           function_code
# MAGIC 
# MAGIC 
# MAGIC Notice the **`def`** keyword, followed by the name of the function, any parameters in parentheses, and a colon. 
# MAGIC 
# MAGIC We've actually already been using functions: **`print()`** is a function that is pre-defined in Python.

# COMMAND ----------

print(1)

# COMMAND ----------

# MAGIC %md <i18n value="d8d44355-44a5-40a3-b8aa-30931e08d346"/>
# MAGIC 
# MAGIC 
# MAGIC As Python executes code, when it sees a call to our function, it jumps to the code block inside the function definition, runs that code, and then jumps back to where the function was called and resumes where it left off. 
# MAGIC 
# MAGIC Let's write a simple example function without any parameters.
# MAGIC 
# MAGIC Let's say we have 10 US Dollars and we want to calculate the conversion from our US Dollars to Euros. As of the writing of this lesson, one dollar is roughly **0.93** Euros.

# COMMAND ----------

def ten_dollars_to_euros():
    print(10.0 * 0.93)
    
ten_dollars_to_euros()

# COMMAND ----------

# MAGIC %md <i18n value="1a38c434-5f42-44fc-9338-b3d92a94548e"/>
# MAGIC 
# MAGIC 
# MAGIC Notice that we indent the code block that is inside the function. Just like with **if-statements**, we must tell Python what code belongs inside the function. Recall that we use **`Tab`** to create the indents.
# MAGIC 
# MAGIC We can call our function as follows. Ignore **arguments** for now as we don't have any in this first example. 
# MAGIC 
# MAGIC ```
# MAGIC function_name(arguments)
# MAGIC ``` 

# COMMAND ----------

print("Python ran this line before the function body")

ten_dollars_to_euros()

print("Python ran this line after the function body")

# COMMAND ----------

# MAGIC %md <i18n value="7511c94b-96a6-462c-89d6-af36b14548d7"/>
# MAGIC 
# MAGIC 
# MAGIC ### Parameters
# MAGIC 
# MAGIC Often, we will want our function to take in some kind of input. Parameters are variables — placeholders for the actual values the function needs. 
# MAGIC 
# MAGIC Let's consider our dollar to euro conversion example. Rather than having our function only convert 10 dollars to euros, it would be better if we could pass in any dollar amount and have that converted to euros. 
# MAGIC 
# MAGIC We can do that by having a parameter representing the **`dollar_amount`** we want the convert.

# COMMAND ----------

def dollars_to_euros(dollar_amount):
    print(dollar_amount * 0.93)

# COMMAND ----------

# MAGIC %md <i18n value="4c927ca4-1ba3-4ee8-a691-9c4a421c99ee"/>
# MAGIC 
# MAGIC 
# MAGIC ### Arguments
# MAGIC 
# MAGIC If our function has parameters, we need to specify what values we want our parameters to have. In our example, we need to provide a value for the **`dollar_amount`** parameter. We do so by including the value within the parentheses when we call the function, just as we did when we provided a value for **`print()`** to display.
# MAGIC 
# MAGIC The value that we pass to our function is called an [**argument**](https://www.w3schools.com/python/gloss_python_function_arguments.asp). In other words, executing **`dollars_to_euros(5)`** assigns the value **`5`** to the **`dollar_amount`** parameter and then runs the function's code.

# COMMAND ----------

dollars_to_euros(5.0)
dollars_to_euros(10)
dollars_to_euros(20.0)

# COMMAND ----------

# MAGIC %md <i18n value="fddb317f-15f3-44ea-bd84-2a05d88b80f4"/>
# MAGIC 
# MAGIC 
# MAGIC ### Multiple Parameters
# MAGIC 
# MAGIC We can create a function with multiple parameters by defining multiple parameters separated by commas.
# MAGIC 
# MAGIC For example, let's specify the **`conversion_rate`** in addition to the **`dollar_amount`** when we call the function, in case it changes in the future.

# COMMAND ----------

def dollars_to_euros_with_rate(dollar_amount, conversion_rate):
    print(dollar_amount * conversion_rate)

# COMMAND ----------

# MAGIC %md <i18n value="19d2ba46-1dd6-48f6-b538-103e590886c4"/>
# MAGIC 
# MAGIC 
# MAGIC When we invoke this new function we must provide a value for each of the function's parameters, separated by commas.

# COMMAND ----------

dollars_to_euros_with_rate(10.0, 0.93)
dollars_to_euros_with_rate(5.0, 1.0)
# dollars_to_euros_with_rate(5.0) # This will error

# COMMAND ----------

# MAGIC %md <i18n value="4e5652da-d220-4ca9-9e79-43913bcab298"/>
# MAGIC 
# MAGIC 
# MAGIC #### Named Invocation
# MAGIC 
# MAGIC Most often, when we pass arguments into a function, we do it as we just did above. We provide a sequence of arguments and they are assigned to the function parameters in the same order.
# MAGIC 
# MAGIC In the call **`dollars_to_euros_with_rate(10, 0.93)`**, **`dollar_amount`** is assigned **`10`** because **`dollar_amount`** is the first parameter and **`10`** is the first argument. Then **`conversion_rate = 0.93`** because they are the second parameter and argument.
# MAGIC 
# MAGIC We can also pass arguments into a function as shown below, explicitly providing the names of the parameters. This is less common, but if done this way the order in which the arguments are passed does not matter.

# COMMAND ----------

dollars_to_euros_with_rate(dollar_amount=10.0, conversion_rate=0.93)
dollars_to_euros_with_rate(conversion_rate=0.93, dollar_amount=10.0)

# COMMAND ----------

# MAGIC %md <i18n value="6bdc03a2-ebb6-4b4d-b5dd-032a434d9694"/>
# MAGIC 
# MAGIC 
# MAGIC ### Default Parameter Values
# MAGIC 
# MAGIC Sometimes it is useful to have [default values](https://www.w3schools.com/python/gloss_python_function_default_parameter.asp) for parameters. 
# MAGIC 
# MAGIC In our dollar to euro conversion example, we might want **`conversion_rate`** to be **`0.93`**, the current conversion rate, unless otherwise specified. 
# MAGIC 
# MAGIC We can define default values for parameters like this: 
# MAGIC 
# MAGIC ```
# MAGIC def func(params, param=default_value):
# MAGIC       code
# MAGIC 
# MAGIC ```

# COMMAND ----------

def dollar_to_euro_with_default(dollar_amount, conversion_rate=0.93):
    print(dollar_amount * conversion_rate)

# COMMAND ----------

# MAGIC %md <i18n value="58cec573-c5dd-48cb-8276-a92362f460f5"/>
# MAGIC 
# MAGIC 
# MAGIC Now when we call this function, if we do not specify an argument for **`conversion_rate`**, it is set to **`0.93`**

# COMMAND ----------

dollar_to_euro_with_default(10.0)
dollar_to_euro_with_default(10.0, 0.5)

# COMMAND ----------

# MAGIC %md <i18n value="95f77560-18ee-44d0-8b8d-1f3648916c31"/>
# MAGIC 
# MAGIC 
# MAGIC ### Function Output
# MAGIC 
# MAGIC So far, all of the functions we have defined only print values. If we evaluate them as an expression, we can see that they don't produce a useful result.

# COMMAND ----------

a = dollar_to_euro_with_default(10.0)
print(a)

# COMMAND ----------

# MAGIC %md <i18n value="02f39e81-a952-4b78-a668-258e8e4b406f"/>
# MAGIC 
# MAGIC 
# MAGIC Our function runs and prints 9.3 while the function body is being executed, but when we try to have Python evaluate the function as an expression, it evaluates to **`None`**. **`None`** is a special data type that represents nothing. 
# MAGIC 
# MAGIC If we want Python to evaluate our function like an expression to the value we are currently printing, we need to use the [**return**](https://www.w3schools.com/python/ref_keyword_return.asp) keyword

# COMMAND ----------

def dollar_to_euro_with_default(dollar_amount, conversion_rate=0.93):
    return dollar_amount * conversion_rate

# COMMAND ----------

a = dollar_to_euro_with_default(10.0)
print(a)

# COMMAND ----------

# MAGIC %md <i18n value="97416e6a-ea97-453c-b6a9-79d314c037d6"/>
# MAGIC 
# MAGIC 
# MAGIC Now, with the **`return`** keyword, Python evaluates **`dollar_to_euro_with_default(10.0)`** to **`0.93`** just like how it evaluates **`10.0 * 0.93`** to **`0.93`**. Anything we want a function to produce to use outside of the function needs to be put after **`return`**. Once Python reaches **`return`** in a function body, it exits the function and jumps back to where it left off.

# COMMAND ----------

# MAGIC %md <i18n value="aa8d0e9c-37c2-4664-a34d-f10b8d125887"/>
# MAGIC 
# MAGIC 
# MAGIC ### Type Hints
# MAGIC 
# MAGIC Notice that we can pass in any type we want as function arguments, even if the function written to work only with a certain type.
# MAGIC 
# MAGIC For example, Python will let us call **`dollars_to_euros_with_default(True, "abc")`**, but it will then fail because multiplication isn't defined between bools and strings.
# MAGIC 
# MAGIC We can add [type hints](https://docs.python.org/3/library/typing.html) to our functions to help with this.
# MAGIC 
# MAGIC This is done by adding a colon, an optional space, and a data type to a parameter like below.
# MAGIC **`dollar_amount: float`**
# MAGIC The return type is indicated with a hyphen, a greater than sign, and data type before the colon at the end of the signature line.
# MAGIC **`-> str:`**
# MAGIC 
# MAGIC For example, if we want to indicate **`dollars_to_euros_with_default`** is only supposed to work with floats and return floats, we can write it as shown below.

# COMMAND ----------

def dollar_to_euro_with_default(dollar_amount: float, conversion_rate: float = 0.93) -> float:
    return dollar_amount * conversion_rate

# COMMAND ----------

# MAGIC %md <i18n value="39b419fb-8600-4ae3-8b0e-f8019137fde8"/>
# MAGIC 
# MAGIC 
# MAGIC It is important to note that these type hints are not enforced. They are hints showing that the types should be, but we can still pass the wrong type into the function and it will try to run it. 
# MAGIC 
# MAGIC Their main benefit is that they improve readability and some coding environments can use them to detect errors earlier.

# COMMAND ----------

# MAGIC %md <i18n value="5c6b9dd2-f667-4272-a2c9-6ecce31df1de"/>
# MAGIC 
# MAGIC 
# MAGIC ### Docstrings
# MAGIC 
# MAGIC Documentation makes your code better organized and more easily understandable by others. A common way to document your code is with [**docstrings**](https://www.geeksforgeeks.org/help-function-in-python/). 
# MAGIC 
# MAGIC Docstrings are special comments that are placed between three quotation marks, as shown below. To use docstrings to document functions, place them in the function body before the function code.

# COMMAND ----------

def dollar_to_euro_with_default(dollar_amount: float, conversion_rate: float = 0.93) -> float:
    """
    Returns Dollar amount to Euros based on a conversion rate
    
    Parameters:
        dollar_amount (float): Dollar amount to be converted to euros
        conversion_rate (float): Dollar to Euro conversion rate. Default:0.93
    
    Returns:
         euro_amount (float): Euro equivalent of Dollar amount based on conversion rate
         """
    euro_amount = dollar_amount * conversion_rate
    return euro_amount

# COMMAND ----------

# MAGIC %md <i18n value="cd94ab20-f634-4dbf-812e-f885691db238"/>
# MAGIC 
# MAGIC 
# MAGIC Docstrings, unlike comments, are saved as a property in Python. The built-in **`help()`** function accesses the docstring and displays it.

# COMMAND ----------

help(dollar_to_euro_with_default)

# COMMAND ----------

# MAGIC %md <i18n value="d5f90617-6c63-4a02-b1c4-af90e815e4a0"/>
# MAGIC 
# MAGIC 
# MAGIC ### Scope
# MAGIC 
# MAGIC In Python, variables defined in certain regions of code are accessible only within the same region. This is referred to as [scope](https://www.w3schools.com/python/python_scope.asp). 
# MAGIC 
# MAGIC It is worth noting that any variable defined within a function is accessible within the function but not accessible outside of the function. In other words, the scope of the variable is limited to the function in which it is defined.

# COMMAND ----------

def function():
    func_variable = 1
    return func_variable

# COMMAND ----------

function()
# func_variable # Uncomment and this will error

# COMMAND ----------

# MAGIC %md <i18n value="00c7e6a3-c466-4242-8d25-c60efacc9175"/>
# MAGIC 
# MAGIC 
# MAGIC ### Built-in Functions
# MAGIC 
# MAGIC Python provides some built-in <a href="https://docs.python.org/3/library/functions.html" target="_blank">functions</a> for common operations. 
# MAGIC 
# MAGIC Some notable ones include **`print()`**, which we have seen, **`max()`** which returns the maximum value of the input and **`len()`** which returns the length of the input.

# COMMAND ----------

# 2 > 1
print(max(1, 2))

# "abc" is 3 characters long
print(len("abc"))

# COMMAND ----------

# MAGIC %md <i18n value="8dca298f-3225-4e6e-b8ad-e84c72c53f46"/>
# MAGIC 
# MAGIC 
# MAGIC We can call **`help()`** on built-in functions to see their documentation.

# COMMAND ----------

help(max)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
