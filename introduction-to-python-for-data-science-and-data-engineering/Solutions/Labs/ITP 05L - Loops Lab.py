# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="7dbd79c5-128b-44de-bfdf-f4ee82cb7df7"/>
# MAGIC 
# MAGIC 
# MAGIC # Loops
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC 
# MAGIC Apply concepts learned in the last lesson, including:
# MAGIC - Utilizing for-loops to handle more advanced control flow
# MAGIC - Using list comprehension to filter lists

# COMMAND ----------

# MAGIC %md <i18n value="c53aee76-7759-4783-b8cc-c615c9db8637"/>
# MAGIC 
# MAGIC  
# MAGIC ## Exercise: Bart Simpson in Detention
# MAGIC 
# MAGIC <img src="https://preview.redd.it/386z0p2eh5v21.jpg?auto=webp&s=383ef3536776dc3a34515e6cfd9979f363570a05" width="40%" height="20%">
# MAGIC 
# MAGIC Bart Simpson got detention again... He needs to write **`I will not let my dog eat my homework`** 50 times. Of course, Bart is lazy, and needs your help to automate this in Python. 
# MAGIC 
# MAGIC Write a function called **`detention_helper()`** that takes in **`detention_message`** and **`num_lines`** representing the message Bart needs to write and the number of times he needs to write it respectively. 
# MAGIC 
# MAGIC Your function should print out **`detention_message`** **`num_lines`** times, but each line of **`detention_message`** should be numbered. 
# MAGIC 
# MAGIC For example, if **`detention_message`** is `I will not let my dog eat my homework`, and **`num_lines`** is 50, the function should print
# MAGIC 
# MAGIC `1. I will not let my dog eat my homework`
# MAGIC 
# MAGIC `2. I will not let my dog eat my homework`
# MAGIC 
# MAGIC `3. I will not let my dog eat my homework`
# MAGIC 
# MAGIC `.
# MAGIC .
# MAGIC .`
# MAGIC 
# MAGIC `50. I will not let my dog eat my homework`
# MAGIC 
# MAGIC 
# MAGIC Here, we are parameterizing the **`detention_message`** and **`num_lines`** in case he gets detention again.
# MAGIC 
# MAGIC **Hint:** Recall the **`range()`** function, but make sure to start counting at 1, not 0. f-string formatting will also be helpful.

# COMMAND ----------

# ANSWER
def detention_helper(detention_message, num_lines):
    for i in range(num_lines):
        print(f"{i+1}. {detention_message}")

# COMMAND ----------

# MAGIC %md <i18n value="5cbae134-c726-4874-b416-a2b802236a6f"/>
# MAGIC 
# MAGIC  
# MAGIC Call your function below with the correct inputs for Bart's current detention and make sure you can see "I will not let my dog eat my homework" printed out 50 times, with the lines numbered, as shown in the problem description.

# COMMAND ----------

# ANSWER
detention_helper("I will not let my dog eat my homework", 50)

# COMMAND ----------

# MAGIC %md <i18n value="75896d4c-b9af-4bd3-acbd-8f7e47d43be0"/>
# MAGIC 
# MAGIC ##Bonus Exercise
# MAGIC Rewrite the for loop above as a while-loop

# COMMAND ----------

# ANSWER
def detention_helper(detention_message, num_lines):
    i=0
    while i < num_lines:
        i += 1
        print(f"{i}. {detention_message}")
        

# COMMAND ----------

# MAGIC %md <i18n value="188c6b2a-e436-4dfa-962d-ae1e8cf733eb"/>
# MAGIC 
# MAGIC Call your function below with the correct inputs for Bart's current detention and make sure you can see "I will do my python homework" printed out 25 times, with the lines numbered, as shown in the problem description.

# COMMAND ----------

# ANSWER
detention_helper("I will do my python homework", 25)

# COMMAND ----------

# MAGIC %md <i18n value="cd5b48af-8534-4d40-a74b-ec9cdd892a33"/>
# MAGIC 
# MAGIC ### Bonus Exercise
# MAGIC 
# MAGIC Below is the code used to display a table of cities and their respective temperatures and humidities from a previous notebook.
# MAGIC 
# MAGIC Modify the code to use lists and loops instead of repetitive variables.

# COMMAND ----------

# TODO: Modify the code below to use lists and loops instead repetitive variables.

# city1 = "San Francisco" # Replace with a list named "cities"
# city2 = "Paris"
# city3 = "Mumbai"

# temperature1 = 58       # Replace with a list named "temperatures"
# temperature2 = 75
# temperature3 = 81

# humidity1 = .85         # Replace with a list named "humidities"
# humidity2 = .5
# humidity3 = .88 

# print(f"{'City':15} {'Temperature':15} {'Humidity':15}")
# print(f"{city1:15} {temperature1:11} {humidity1:12.2f}")
# print(f"{city2:15} {temperature2:11} {humidity2:12.2f}")
# print(f"{city3:15} {temperature3:11} {humidity3:12.2f}")

# COMMAND ----------

# ANSWER: Option 1
city_list = ["San Francisco", "Paris", "Mumbai"]
temperature_list = [58, 75, 81]
humidity_list = [.85, .5, .88]

print(f"{'City':15} {'Temperature':15} {'Humidity':15}")
i = 0
while (i < len(city_list)):
    print(f"{city_list[i]:15} {temperature_list[i]:11} {humidity_list[i]:12.2f}")
    i = i + 1

# COMMAND ----------

# ANSWER: Option 2
list = [["San Francisco", 58, .85], ["Paris", 75, .5], ["Mumbai", 81, .88]]

print(f"{'City':15} {'Temperature':15} {'Humidity':15}")
for i in list:
    print(f"{i[0]:15} {i[1]:11} {i[2]:12.2f}")

# COMMAND ----------

# MAGIC %md <i18n value="7799d859-cb3c-46ac-87e2-414d95de6fcf"/>
# MAGIC 
# MAGIC ### Bonus Exercise
# MAGIC 
# MAGIC Write a function named `item_count` that accepts a list of values and returns a dictionary with a count of the number of times each unique value appeared in the list
# MAGIC - For example, `item_count(['a', 'b', 'a'])` should return the dictionary `{'a': 2, 'b': 1}`

# COMMAND ----------

# ANSWER
def item_count(input_list):
    output_dict = {}  # Initialize an empty dictionary
  
    for item in input_list:
        if item not in output_dict:
      # Create an element for the item with an initial count of 1
            output_dict[item] = 1
    else:
      # Add 1 to the current count for the item
        output_dict[item] += 1
      
    return output_dict

# COMMAND ----------

assert item_count(['a', 'b', 'a']) == {'a': 2, 'b': 1}, "There should be 2 occurrences of the letter 'a' and one occurrence of the letter 'b'"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
