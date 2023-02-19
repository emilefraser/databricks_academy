# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="2e588f6c-a1c4-4184-888c-25dd7f246cf4"/>
# MAGIC 
# MAGIC 
# MAGIC # Collection Types and Methods
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC 
# MAGIC Apply concepts learned in the last lesson, including:
# MAGIC - Using objects and methods with new collection data types

# COMMAND ----------

# MAGIC %md <i18n value="f43f90db-5f06-4f7b-bbe1-e4a4a27a1984"/>
# MAGIC 
# MAGIC 
# MAGIC #### Problem 1a: Dinner Foods
# MAGIC 
# MAGIC We provide a list called **`dinner_list`** with elements "potatoes, peppers, onions" in that order.

# COMMAND ----------

# TODO
dinner_list = TODO

# COMMAND ----------

# MAGIC %md <i18n value="5382a8a7-3300-468e-8f1d-8009c6c3bb8c"/>
# MAGIC 
# MAGIC 
# MAGIC #### Problem 1b: Dinner Foods 
# MAGIC 
# MAGIC However, we actually ate sweet potatoes, not normal potatoes, so change the first element of the list to be "sweet potatoes".

# COMMAND ----------

# TODO

# COMMAND ----------

assert dinner_list == ['sweet potatoes', 'peppers', 'onions'], "dinner_list should be ['sweet potatoes', 'peppers', 'onions']"
print("Test passed!")

# COMMAND ----------

# MAGIC %md <i18n value="768e4c64-8466-4084-87b0-51a55f63c26f"/>
# MAGIC 
# MAGIC  
# MAGIC #### Problem 1c: Dinner Foods 
# MAGIC Finally, we also ate some rice, so add "rice" to the end of the list.

# COMMAND ----------

# TODO

# COMMAND ----------

assert dinner_list == ['sweet potatoes', 'peppers', 'onions', 'rice'], "dinner_list should be ['sweet potatoes', 'peppers', 'onions', 'rice']"
print("Test passed!")

# COMMAND ----------

# MAGIC %md <i18n value="04208057-4d9e-4b91-a12c-1c811d2940bf"/>
# MAGIC 
# MAGIC 
# MAGIC #### Problem 2a: Dinner Dictionaries
# MAGIC 
# MAGIC Create a dictionary called **`dinner_dict`** with pairs `"sweet potatoes": 3`, `"peppers": 4`, `"onions": 1` representing what we ate for dinner.

# COMMAND ----------

# TODO

# COMMAND ----------

assert dinner_dict["sweet potatoes"] == 3, "We had 3 sweet potatoes"
assert dinner_dict["peppers"] == 4, "We had 4 peppers"
assert dinner_dict["onions"] == 1, "We had 1 onion"
print("Tests passed!")

# COMMAND ----------

# MAGIC %md <i18n value="01f79df2-b253-4e4d-949b-868d08287636"/>
# MAGIC 
# MAGIC 
# MAGIC #### Problem 2b: Updating Dinner Dictionary
# MAGIC 
# MAGIC After thinking about it, we realize we actually only had 2 `sweet potatoes`. And while we didn't want to admit it, we also had one `ice cream` as well.
# MAGIC 
# MAGIC Update the dictionary to reflect those changes.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md <i18n value="ea6adf37-9c45-44fa-a1ae-9f3452f05db8"/>
# MAGIC 
# MAGIC 
# MAGIC **Check your work:**

# COMMAND ----------

assert dinner_dict["sweet potatoes"] == 2, "We had 2 sweet potatoes"
assert dinner_dict["ice cream"] == 1, "We had 1 ice cream (but don't tell!)"
print("Tests passed!")

# COMMAND ----------

# MAGIC %md <i18n value="0d94c470-0a24-4071-8e23-d71d610eb80e"/>
# MAGIC 
# MAGIC 
# MAGIC ### Problem 3: Sets
# MAGIC 
# MAGIC A very valuable skill for a programmer to have is to be able to look at documentation for a data type they do not know and understand how to use it. 
# MAGIC 
# MAGIC With that in mind this exercise asks you to explore a new collection data type called **Sets**. 
# MAGIC 
# MAGIC Use the documentation provided <a href="https://docs.python.org/3/tutorial/datastructures.html#sets" target="_blank">here</a> to complete the next problem.
# MAGIC 
# MAGIC Create the following sets:
# MAGIC   * **`ingredient_set_1`** with the ingredients "carrots", "onions", and "potatoes".
# MAGIC   * **`ingredient_set_2`** with the ingredients "broccoli", "carrots", and "rice".
# MAGIC   * **`ingredient_set_3`** with the ingredients "sweet potatoes", "carrots", and "corn".
# MAGIC 
# MAGIC Programmatically create a new set **`ingredient_intersection_set`** which contains only those ingredients that occur in all three sets.

# COMMAND ----------

# TODO
ingredient_set_1 = TODO
ingredient_set_2 = TODO
ingredient_set_3 = TODO
ingredient_intersection_set = TODO

# COMMAND ----------

# MAGIC %md <i18n value="4f4f2578-063d-41c5-bffb-3fefadbe7380"/>
# MAGIC 
# MAGIC 
# MAGIC **Check your work:**

# COMMAND ----------

assert ingredient_intersection_set == {"carrots"}, "Only carrots occurs in all the ingredients"
assert "broccoli" in ingredient_set_2, "Did you forget your broccoli?"

print("Test passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
