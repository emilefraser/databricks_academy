# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="13cdba3e-bd9b-441f-9202-12e26ce9135f"/>
# MAGIC 
# MAGIC 
# MAGIC # Loops
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC 
# MAGIC - Explore for-loops, a new way to change control flow
# MAGIC - Use for-loops to filter lists

# COMMAND ----------

# MAGIC %md <i18n value="f5c07310-2b29-4266-9ec2-9b239ebae74f"/>
# MAGIC 
# MAGIC 
# MAGIC ### For-loops
# MAGIC 
# MAGIC Loops are a way to repeat a block of code while iterating over a sequence.
# MAGIC 
# MAGIC We write a [**for-loop**](https://www.w3schools.com/python/python_for_loops.asp) like this:
# MAGIC ```
# MAGIC  for var_name in list:
# MAGIC      code_block
# MAGIC ```
# MAGIC 
# MAGIC Python will execute the code in the **`code_block`** once per item in the list. Each time, it will assign **`var_name`** to be the next item in the list, starting at the beginning.

# COMMAND ----------

# MAGIC %md <i18n value="ac31dae5-adc1-4167-b689-87d6bce353e2"/>
# MAGIC 
# MAGIC 
# MAGIC Each time the code loops, **`var_name`**, which is **`number`** in the example below, is set equal to the next item in the list. 
# MAGIC 
# MAGIC Let's break this down:
# MAGIC 
# MAGIC Step 1. **`number = 0`**, the first element in the list, and prints **`0`**.
# MAGIC 
# MAGIC Step 2. **`number`** is set to the next element in the list, so **`number = 1`**, and prints **`1`**.
# MAGIC 
# MAGIC Step 3. **`number`** is set to the next element in the list again, so **`number = 2`**, and prints **`2`**.
# MAGIC 
# MAGIC Using this, we can have the code in the **`for`** loop act on every item in the list.

# COMMAND ----------

for number in [0, 1, 2]:
    print(number)

# COMMAND ----------

# MAGIC %md <i18n value="90b9748a-7ecc-440c-98ca-5acde97b5212"/>
# MAGIC 
# MAGIC 
# MAGIC If you want to execute a code block many times but don't need to iterate over a list, you can instead use [**range()**](https://www.w3schools.com/python/ref_func_range.asp).
# MAGIC 
# MAGIC **`range()`** takes in a start and a stop index (stop index value is exclusive, not inclusive). By default, it will increment one at a time starting at start and ending at stop-1. So **`range(0, 4)`** would iterate over the values: 0, 1, 2, 3.
# MAGIC 
# MAGIC For example, let's print **`"Hello!"`** 10 times.

# COMMAND ----------

for element in range(0, 10):
    print("Hello!")

# COMMAND ----------

# MAGIC %md <i18n value="4e3f1154-8637-45de-a4ae-5b285bb87fdd"/>
# MAGIC 
# MAGIC 
# MAGIC Here, **`element`** is temporarily assigned to each number in that range at each iteration.

# COMMAND ----------

for element in range(0, 10):
    print(element)

# COMMAND ----------

# MAGIC %md <i18n value="1ff92a97-049b-44a0-ab26-dac4a85ca615"/>
# MAGIC 
# MAGIC 
# MAGIC Question: How can we change the code to print 1-10, not 0-9?

# COMMAND ----------

# MAGIC %md <i18n value="edd56f68-c314-46f0-9d37-de1ef33a449e"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC You can use loops to filter a list. For example, let's say we wanted to filter a list of numbers to only keep the numbers greater than 4. 
# MAGIC 
# MAGIC We can accomplish this by creating an empty new list, looping over our list of numbers, and adding numbers to the empty list if they are greater than 4.

# COMMAND ----------

numbers_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
final_list = []

for element in numbers_list:
    if element > 4:
        final_list.append(element)
    else:
        pass # do nothing
        
final_list

# COMMAND ----------

# MAGIC %md <i18n value="cb72b94d-ff2f-43ba-8468-5666faa713f3"/>
# MAGIC 
# MAGIC 
# MAGIC Creating a new list from filtering one with a loop is a common enough problem that Python actually provides a very useful shortcut. 
# MAGIC 
# MAGIC The shortcut is called [list comprehension](https://www.w3schools.com/python/python_lists_comprehension.asp) and it looks like this: **`[var_name for var_name in list if (boolean condition)]`**
# MAGIC 
# MAGIC Let's use this shortcut to do the exact same thing we did in the previous cell.

# COMMAND ----------

final_list_shortcut = [element for element in numbers_list if element > 4]
final_list_shortcut

# COMMAND ----------

# MAGIC %md <i18n value="42f3bd09-15b7-4cb6-bf4d-61d867f57f8c"/>
# MAGIC 
# MAGIC 
# MAGIC You can read this left to right as including **`element`** in the final list for each **`element`** in **`numbers`** if the **`element`** is greater than 4.
# MAGIC 
# MAGIC For example, let's say instead of just including every **`element`** in **`numbers`** that is greater than 4, we instead want to include **`2 * element`** for every **`element`** in **`numbers`** if **`element`** if greater than 4. Let's look at the code below.

# COMMAND ----------

doubled_list = [2 * element for element in numbers_list if element > 4]
doubled_list

# COMMAND ----------

# MAGIC %md <i18n value="9d0bf7b0-5558-47fe-9a02-342e7f644c66"/>
# MAGIC 
# MAGIC 
# MAGIC The boolean expression is actually optional. Let's double every element in the list.

# COMMAND ----------

[2 * element for element in numbers_list]

# COMMAND ----------

# MAGIC %md <i18n value="07060457-fa2d-48b6-b140-c4dbc681af0d"/>
# MAGIC 
# MAGIC 
# MAGIC ### `break`
# MAGIC 
# MAGIC If you want to exit a loop early, before the loop has finished iterating over it's sequence, you can use **`break`**.
# MAGIC 
# MAGIC **`break`** is written on it's own line inside a loop code block, and when Python executes that line, Python will exit the loop code block and stop iterating over the list. 
# MAGIC 
# MAGIC Let's use this to stop iterating over **`numbers_list`** once we reach the number 4.

# COMMAND ----------

for element in numbers_list:
    if element == 4:
        break
    print(element)

# COMMAND ----------

# MAGIC %md <i18n value="0abb7db5-41c8-4c67-ba08-664507255fa6"/>
# MAGIC 
# MAGIC 
# MAGIC ### `continue`
# MAGIC 
# MAGIC **`break`** exits the loop code block when it is executed and stops iterating over the list. If instead you wanted to exit the loop code block early, but still move on and continue to execute the sequence, you could use **`continue`** instead. 
# MAGIC 
# MAGIC **`continue`** is also written on it's own line and when it is executed Python stops executing the loop code block and then continues to iterate over the sequence.

# COMMAND ----------

for element in numbers_list:
    if element == 4:
        continue # 4 is not printed, but the numbers after are
    print(element)

# COMMAND ----------

# MAGIC %md <i18n value="65b05201-3344-43a1-aeee-33911d306bef"/>
# MAGIC 
# MAGIC 
# MAGIC ### While-loops
# MAGIC 
# MAGIC In addition to for-loops there is another kind of loops called while-loops. 
# MAGIC 
# MAGIC We write a [**while-loop**](https://www.w3schools.com/python/python_while_loops.asp) like this:
# MAGIC ```
# MAGIC  while boolean expression:
# MAGIC      code_block
# MAGIC ```
# MAGIC 
# MAGIC Python will loop and execute the code in the **`code_block`** until the boolean expression evaluates to **`False`**. Every loop, it will reevaluate the boolean expression, and if it is **`True`** it will execute the code again, otherwise it will exit. 
# MAGIC 
# MAGIC **NOTE:** You need to be careful not to have infinite loops here. If the boolean expression never evaluates to **`False`**, this code will keep running and never stop.

# COMMAND ----------

count = 10

while count > 0:
    print(count)
    count = count - 1

# COMMAND ----------

# MAGIC %md <i18n value="ddc22fc8-902d-49e5-baa9-62e61cc2f6d8"/>
# MAGIC 
# MAGIC ### More fun with repetition
# MAGIC 
# MAGIC Python also supports nested loops and ranges.

# COMMAND ----------

for a in range(0,10):
    for b in range(0,10):
        for c in range(0,10):
            print(str(a) +  str(b) + str(c))

# COMMAND ----------

# MAGIC %md <i18n value="4d276a26-f723-4ccc-bfe5-3ad2c0b14f90"/>
# MAGIC 
# MAGIC If it is not already apparent, the general beauty of a loop is that it decouples the amount of code you write from the size of the data being processed.
# MAGIC - You can process a list or range of 50,000,000 elements with no more code than it takes to process a list or range of 5 elements.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
