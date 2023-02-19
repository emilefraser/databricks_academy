# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="5e2b0b7f-f62f-49f4-9178-aa14bf84b5b7"/>
# MAGIC 
# MAGIC 
# MAGIC # Control Flow
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC 
# MAGIC  - Alter the control flow of a Python program
# MAGIC  - Combine boolean expressions with conditional statements

# COMMAND ----------

# MAGIC %md <i18n value="adcf81b0-d6b9-4d96-8952-8535f6b05741"/>
# MAGIC 
# MAGIC 
# MAGIC ## if-statement
# MAGIC 
# MAGIC Python by default evaluates every line of code from top to bottom, sequentially. But what if we want to define conditional logic to control which code should be executed? 
# MAGIC 
# MAGIC The order in which Python runs our code is called **control flow**, and Python provides functionality we can use to change the control flow. 
# MAGIC 
# MAGIC The first tool Python provides for us is the **if-statement**. 
# MAGIC 
# MAGIC We write an [**if-statement**](https://www.w3schools.com/python/gloss_python_if_statement.asp) like this:
# MAGIC 
# MAGIC 
# MAGIC     if bool:
# MAGIC         code_1
# MAGIC     else:
# MAGIC         code_2
# MAGIC       
# MAGIC `bool` here is a boolean expression, evaluating to either **`True`** or **`False`**
# MAGIC 
# MAGIC Think of this as a fork in the road. Python will read the boolean at the top, then if it is **`True`**, it will execute **`code_1`** not **`code_2`**
# MAGIC 
# MAGIC If it is **`False`**, it will skip **`code_1`** and only execute **`code_2`**
# MAGIC 
# MAGIC Let's look at some examples

# COMMAND ----------

if True:
    print("True")
else:
    print("False")

# COMMAND ----------

# A more practical example
# Enter a number (at the top of the display)

dbutils.widgets.text("input", "1", "Enter a number from 1 - 10")  #We're setting 1 as the default value
input = dbutils.widgets.get("input")

# COMMAND ----------

# Evaluate the input

input_as_number = int(input)
if input_as_number >= 1 and input_as_number <= 10:
    print("Valid Input")
else:
    print("Invalid Input")
    
# Remove the widget
dbutils.widgets.remove("input")

# COMMAND ----------

# MAGIC %md <i18n value="d521ae49-84e2-42e1-88e2-ec13549e726d"/>
# MAGIC 
# MAGIC 
# MAGIC ### Indentation
# MAGIC 
# MAGIC Notice the [indentation](https://www.w3schools.com/python/gloss_python_if_indentation.asp) in the **if-statement**. While this does make the code more legible, **this is actually necessary for the code to run**.
# MAGIC 
# MAGIC Python reads the indentations to understand what code is inside the **if** and **else** blocks. 
# MAGIC 
# MAGIC According to Python standards, indentation should be 4 spaces, but the **`Tab`** key is a short cut.

# COMMAND ----------

# MAGIC %md <i18n value="694f3cf9-552d-4daa-ab39-8f68e0d28514"/>
# MAGIC 
# MAGIC 
# MAGIC ## Operators
# MAGIC 
# MAGIC Now that we can use **if-statements**, let's take a look at some boolean expressions we can use with them. 
# MAGIC 
# MAGIC Recall the boolean operators **`and`**, **`or`**, and **`not`**.
# MAGIC 
# MAGIC Python will evaluate the boolean expression of an **if-statement** to **`True`** or **`False`**, and then act accordingly. 
# MAGIC 
# MAGIC So far we have only seen expressions composed of one type that evaluate to the same type.
# MAGIC 
# MAGIC For example:
# MAGIC 
# MAGIC **`True or False -> True`**
# MAGIC 
# MAGIC **`1 + 2 -> 3`**
# MAGIC 
# MAGIC We can also have expressions composed of one type that evaluate to a different type. 
# MAGIC 
# MAGIC We'll look at the **`<`**, **`>`**, **`<=`**, **`>=`**, **`==`**, and **`!=`** operators, which are defined for multiple other types but evaluate to booleans. 
# MAGIC 
# MAGIC 
# MAGIC | <    | > | <= | >= | == | != |
# MAGIC | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
# MAGIC | Less than   | Greater than    | Less than or equal to     | Greater than or equal to    | Equal to     | Not equal to      |
# MAGIC | **`a < b -> bool`**    | **`a > b -> bool`**        |**`a <= b -> bool`**   | **`a >= b -> bool`**       |**`a == b -> bool`**   | **`a != b -> bool`**      |
# MAGIC 
# MAGIC **Note:** **`=`** is used for variable assignment, **`==`** is used for equality comparison

# COMMAND ----------

print(1 == 1)
print(1.5 != 2.5)
print("abc" == "xyz")
print(True == True)

# COMMAND ----------

# MAGIC %md <i18n value="12aa040d-a993-49d0-b3c8-a19fbc6b0a12"/>
# MAGIC 
# MAGIC 
# MAGIC Let's use these operators to see if we should buy lunch based on its price. We'll have a budget of 15 dollars for lunch, but might be willing to go a bit over that if the food is really good.

# COMMAND ----------

lunch_price = 20

if lunch_price <= 15:
    print("Buy it!")
else:
    print("Too expensive")
    

# COMMAND ----------

# MAGIC %md <i18n value="a634486b-b213-4e83-b920-288aa64c0dc8"/>
# MAGIC 
# MAGIC 
# MAGIC The code inside the **if** or **else** block can be anything, even another **if statement**.

# COMMAND ----------

if lunch_price <= 15:
    print("Buy it!")
else:
    if lunch_price < 25:
        print("Is it really good?")
    else:
        print("This better be the best food of all time")

# COMMAND ----------

# MAGIC %md <i18n value="ae3c161b-53c5-4e87-99af-43ddf67ea92c"/>
# MAGIC 
# MAGIC 
# MAGIC ## elif
# MAGIC 
# MAGIC We can expand if-statements to consider multiple boolean expressions by adding **elif statements**.
# MAGIC 
# MAGIC We write them as follows:
# MAGIC 
# MAGIC 
# MAGIC     if bool:
# MAGIC         code_1
# MAGIC     elif bool:
# MAGIC         code_2
# MAGIC     elif bool:
# MAGIC         code_3
# MAGIC     .
# MAGIC     .
# MAGIC     .
# MAGIC     else:
# MAGIC         code_last
# MAGIC       
# MAGIC We can include multiple **elif statements**. 
# MAGIC 
# MAGIC Python evaluates the boolean expressions starting from the top. If a boolean expression evaluates to **`True`**, Python executes the commands in the following code block and then skips all subsequent **elif** and **else** clauses.
# MAGIC 
# MAGIC If none of the boolean expressions evaluate to **`True`**, Python runs the **else** code block.

# COMMAND ----------

lunch_price = 15

if lunch_price == 10:
    print("10 dollars exactly! Buy it!")
elif lunch_price <= 15:
    print("Buy it!")
elif lunch_price < 25:
    print("Is it really good?")
else:
    print("This better be the best food of all time")

# COMMAND ----------

# MAGIC %md <i18n value="82b070bf-589d-4791-b85c-66f51b170b76"/>
# MAGIC 
# MAGIC 
# MAGIC ## Dog Breed Recommendations
# MAGIC 
# MAGIC Suppose we are helping people to pick a dog breed where users provide the following information:
# MAGIC 
# MAGIC * **`dog_person`**: Boolean representing if they are a dog person or not
# MAGIC * **`cat_person`**: Boolean representing if they are a cat person or not
# MAGIC * **`age`**: The user's age
# MAGIC 
# MAGIC Based on the user's input to these questions, we print out a recommendation. 
# MAGIC 
# MAGIC Our application works like this:
# MAGIC 
# MAGIC * If the user is not an adult (under the age of 18), we tell them to ask their parents for permission. 
# MAGIC 
# MAGIC * Otherwise, 
# MAGIC 
# MAGIC   * If they are both a dog person and a cat person, we recommend Golden Retriever, since they get along well with cats.
# MAGIC   * If they are a dog person, but not a cat person, we recommend Scottish Deerhound, since they are known to chase cats.
# MAGIC   * If they are a cat person, but not a dog person, we tell them they're barking up the wrong tree.
# MAGIC   * Finally, if they are neither a dog person or a cat person, we ask them to evaluate whether a pet is right for them.

# COMMAND ----------

dog_person = True
cat_person = True
age = 30

if age < 18:
    print("Ask your parents for permission!")
else:
    if dog_person and cat_person: # implicitly evaluates dog_person == True and cat_person == True, so can omit the == True
        print("Golden Retriever")
    elif dog_person and not cat_person:
        print("Scottish Deerhound")
    elif cat_person and not dog_person:
        print("You're barking up the wrong tree!")
    else:
        print("Are you sure a pet is right for you?")

# COMMAND ----------

# MAGIC %md <i18n value="5ba1204e-82df-4060-8c87-a981b3843075"/>
# MAGIC 
# MAGIC 
# MAGIC We could also write it as shown below, making sure we are mindful of the order of boolean conditions. Remember that at the first **`True`** Python will stop.

# COMMAND ----------

dog_person = True
cat_person = True
age = 30

if age < 18:
    print("Ask your parents for permission!")
elif dog_person and cat_person:
    print("Golden Retriever")
elif dog_person and not cat_person:
    print("Scottish Deerhound")
elif cat_person and not dog_person:
    print("You're barking up the wrong tree!")
else:
    print("Are you sure a pet is right for you?")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
