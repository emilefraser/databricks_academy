# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="d845488d-8461-4ee9-8b24-9bdff213d1fb"/>
# MAGIC 
# MAGIC 
# MAGIC # Collection Types and Methods
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC 
# MAGIC - Introduce objects and methods
# MAGIC - Create lists
# MAGIC - Use methods on new collection data types

# COMMAND ----------

# MAGIC %md <i18n value="2d0178c2-f2b4-4746-82eb-5b81eec6be0e"/>
# MAGIC 
# MAGIC 
# MAGIC ## Objects
# MAGIC 
# MAGIC In this lesson we are first going to look at some new functionality provided by data types, and then see how we can use that in some new data types. But before we do that, we need to look at some terminology.
# MAGIC 
# MAGIC An [**object**](https://www.w3schools.com/python/python_classes.asp) is an instance of a specific data type. 
# MAGIC 
# MAGIC For example, **`1`** is an Integer, so we would call it an Integer object. **`"Hello"`** is a String, so we would call it a String object.

# COMMAND ----------

# MAGIC %md <i18n value="753edb8e-797e-4bf2-b6f4-4d4c952512a1"/>
# MAGIC 
# MAGIC 
# MAGIC ## Methods: More Functionality
# MAGIC 
# MAGIC As a reminder, data types provide **data** of some kind and **operations** we can do on that kind of data. So far, we have actually only looked at a small fraction of the operations provided by each type. 
# MAGIC 
# MAGIC Data types provide special functions called [**methods**](https://www.w3schools.com/python/gloss_python_object_methods.asp) which provide more functionality. Methods are exactly like normal functions except we call them on objects and they can edit the object they are called on. We invoke a method like this:
# MAGIC 
# MAGIC **`object.method_name(arguments)`**
# MAGIC 
# MAGIC This is a little tricky and we have a whole lesson on these coming up, but right now all you need to know is:
# MAGIC 
# MAGIC **Methods are functions provided by a data type that we can call on objects of that type. They act on the object we call them with and allow us to use more functionality provided by that data type**

# COMMAND ----------

# MAGIC %md <i18n value="2ca5b59c-4cb8-4e87-b648-166eee0aedad"/>
# MAGIC 
# MAGIC 
# MAGIC ### String Methods
# MAGIC 
# MAGIC Let's take a look at an example of a method on a type we already know well: Strings. Strings provide a method called [**upper()**](https://www.w3schools.com/python/ref_string_upper.asp) which capitalizes a String.

# COMMAND ----------

greeting = "hello"
print(greeting.upper())
print(greeting)

# COMMAND ----------

# MAGIC %md <i18n value="b83c4e97-70e0-4104-90f1-12ede96a8515"/>
# MAGIC 
# MAGIC 
# MAGIC ### In-place methods
# MAGIC 
# MAGIC Methods are functions that act on objects, and can either perform operations in-place (modify the underlying object it was called upon) or return a new object.
# MAGIC 
# MAGIC Notice that the method **`upper()`** was not a stateful, in-place method as it returned a new string and did not modify the **`greeting`** variable. Take a look <a href="https://www.w3schools.com/python/python_ref_string.asp" target="_blank">W3Schools</a> provides information on other string methods in Python.

# COMMAND ----------

# MAGIC %md <i18n value="bc7aa58a-5ea7-4387-96b7-189b364d56c3"/>
# MAGIC 
# MAGIC 
# MAGIC ### Tab Completion
# MAGIC 
# MAGIC If you want to see a list of methods you can apply to an object, type **`.`** after the object, then hit tab key to see a drop down menu of available methods on that object.
# MAGIC 
# MAGIC Try it below on the **`greeting`** string object. Type **`greeting.`** then hit the Tab key.

# COMMAND ----------

# Type . and hit Tab
greeting

# COMMAND ----------

# MAGIC %md <i18n value="5b3151ca-f1d4-430f-8129-3cb28816c364"/>
# MAGIC 
# MAGIC 
# MAGIC ### `help()`
# MAGIC 
# MAGIC While using tab completion is extremely helpful, if we use it to look through all possible methods for a given object, we might still not be certain how those methods work.
# MAGIC 
# MAGIC We can look up their documentation, or we can use the [**help()**](https://www.geeksforgeeks.org/help-function-in-python/) function we saw last lesson.
# MAGIC 
# MAGIC As a reminder, the **`help()`** function displays some of the documentation for the item passed into it.
# MAGIC 
# MAGIC For example, when using tab completion above, we see the [**capitalize()**](https://www.w3schools.com/python/ref_string_capitalize.asp) string method, but we are not certain how it works.

# COMMAND ----------

help(greeting.capitalize)

# COMMAND ----------

greeting.capitalize()

# COMMAND ----------

# MAGIC %md <i18n value="238070d2-fa76-43ef-a14b-2d67ef88af08"/>
# MAGIC 
# MAGIC 
# MAGIC ## Methods with Collection Types
# MAGIC 
# MAGIC Now that we have a brief understanding of methods, let's look at some more advanced data types and the methods they provide.
# MAGIC 
# MAGIC We are going to look at **collection data types** next. Like the name suggests, the data in these data types is a collection of other data types.

# COMMAND ----------

# MAGIC %md <i18n value="d028dd16-da40-4c7d-b62e-650e154d1710"/>
# MAGIC 
# MAGIC 
# MAGIC ### Collection Type 1: Lists
# MAGIC 
# MAGIC A list is just an ordered sequence of items. 
# MAGIC 
# MAGIC It is defined as a sequence of comma separated items inside square brackets like this: **`[item1, item2, item3...]`**
# MAGIC 
# MAGIC The items may be of any type, though in practice you'll usually create lists where all of the values are of the same type.
# MAGIC 
# MAGIC Let's make a <a href="https://www.w3schools.com/python/python_lists.asp" target="_blank">list</a> of what everyone ate for breakfast this morning.
# MAGIC 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/2/20/Scrambed_eggs.jpg/1280px-Scrambed_eggs.jpg" width="20%" height="10%">

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list

# COMMAND ----------

# Python can tell us breakfast_list's type
type(breakfast_list)

# COMMAND ----------

# MAGIC %md <i18n value="5ed35859-d9a4-44e0-a152-befc8855bab0"/>
# MAGIC 
# MAGIC 
# MAGIC We'll use our **`breakfast_list`** as the running example, but note that the values in a list can be of any type, as shown below.

# COMMAND ----------

# any type works
["hello", True, 1, 1.5]

# COMMAND ----------

# MAGIC %md <i18n value="5bd3521a-76df-46d3-b5ec-07d22a896321"/>
# MAGIC 
# MAGIC 
# MAGIC #### List Methods
# MAGIC 
# MAGIC Now that we understand the **data** a list data type provides, let's look at some of its **functionality**.
# MAGIC 
# MAGIC Something you will frequently want to do is add a new item to an existing list. 
# MAGIC 
# MAGIC Lists provide a method called [**append()**](https://www.w3schools.com/python/ref_list_append.asp) to do just that. 
# MAGIC 
# MAGIC **`append()`** takes in an argument of any type and edits the list it is called on so that the argument is stuck onto the end of the list. 
# MAGIC 
# MAGIC Let's say after we ate our pancakes, eggs, and waffles, we also had yogurt.
# MAGIC 
# MAGIC Here, we can use **`append()`** to add yogurt to our **`breakfast_list`**.

# COMMAND ----------

breakfast_list.append("yogurt")
breakfast_list

# COMMAND ----------

# MAGIC %md <i18n value="668b2cdd-8135-439b-bf95-2880346ce49c"/>
# MAGIC 
# MAGIC 
# MAGIC **Note:** Notice here that **`append()`** is an in-place method.
# MAGIC The method does not return a new list, but rather edits the original **`breakfast_list`** object. 
# MAGIC 
# MAGIC **`+`** is also defined as concatenation for lists as shown below.

# COMMAND ----------

["pancakes", "eggs"] + ["waffles", "yogurt"]

# COMMAND ----------

# MAGIC %md <i18n value="6237eb3c-7a8b-428b-9418-4b397c163484"/>
# MAGIC 
# MAGIC 
# MAGIC While we typically use **`append()`**, it is possible to append elements to a list using **`+`**.

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list = breakfast_list + ["yogurt"]
breakfast_list

# COMMAND ----------

# MAGIC %md <i18n value="a4bcc594-73a3-454d-ab4d-83db2844db10"/>
# MAGIC 
# MAGIC 
# MAGIC A useful shortcut operation for this is **`+=`**.
# MAGIC 
# MAGIC **`breakfast_list`** `+=` **`["yogurt"]`** is the same thing is **`breakfast_list`** `=` **`breakfast_list`** `+` **`["yogurt"]`**.
# MAGIC 
# MAGIC The **`+=`** operator works for other types as well, using their respective **`+`** operator.

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list += ["yogurt"]
breakfast_list

# COMMAND ----------

# MAGIC %md <i18n value="31cf815e-ec37-4313-b36c-84023b9a6f8c"/>
# MAGIC 
# MAGIC 
# MAGIC #### List indexing
# MAGIC 
# MAGIC Often, we want to reference a specific item or items in a list. This called [list indexing](https://www.w3schools.com/python/python_lists_access.asp).
# MAGIC 
# MAGIC Lists provide an operation to get the item at a certain index in the list like this:
# MAGIC 
# MAGIC       list_name[index]
# MAGIC 
# MAGIC In Python indices start from 0, so the first element of the list is 0, the second is 1, etc.

# COMMAND ----------

breakfast_list[0]

# COMMAND ----------

# MAGIC %md <i18n value="b86f1c49-9cc7-4699-a329-2e60bc68a774"/>
# MAGIC 
# MAGIC 
# MAGIC We can also use negative indexing, which starts counting from right to left, starting from -1. 
# MAGIC 
# MAGIC Thus, the last element of the list is -1, the second to last is -2, etc.

# COMMAND ----------

breakfast_list[-1]

# COMMAND ----------

# MAGIC %md <i18n value="7cefd54e-7d6b-449e-99c0-48809deafc78"/>
# MAGIC 
# MAGIC 
# MAGIC We can also provide a range of indices we want to access like this:
# MAGIC 
# MAGIC **`list_name[start:stop]`**
# MAGIC 
# MAGIC This returns a list of the values starting at **`start`** and up to but not including **`stop`**.

# COMMAND ----------

# Note the stop index is exclusive
breakfast_list[0:2]

# COMMAND ----------

# MAGIC %md <i18n value="bd802cdb-aece-435f-8056-151ab60ce5a7"/>
# MAGIC 
# MAGIC 
# MAGIC If we don't provide a start index, Python assumes we start at the beginning.
# MAGIC 
# MAGIC If we don't provide a stop index, Python assumes we stop at the end.

# COMMAND ----------

print(breakfast_list[:2])
print(breakfast_list[1:])

# COMMAND ----------

# MAGIC %md <i18n value="c1232cd7-3bd1-4dad-bc96-97ed5d36c5b1"/>
# MAGIC 
# MAGIC 
# MAGIC We can also change the value of an index in a list to be something new like this:

# COMMAND ----------

print(breakfast_list)
breakfast_list[0] = "sausage"

print(breakfast_list)

# COMMAND ----------

# MAGIC %md <i18n value="b26f49c7-544d-4cd7-811b-5439f136cdbe"/>
# MAGIC 
# MAGIC 
# MAGIC We can also use **`in`** to check if an element is in a given list. This is a boolean operation:

# COMMAND ----------

"waffles" in breakfast_list

# COMMAND ----------

# MAGIC %md <i18n value="589d7a85-089c-4870-abe7-7ad2fd293698"/>
# MAGIC 
# MAGIC 
# MAGIC ### Collection Type 2: Dictionaries
# MAGIC 
# MAGIC A [Dictionary](https://www.w3schools.com/python/python_dictionaries.asp) is a sequence of key-value pairs. We define a dictionary as follows:
# MAGIC 
# MAGIC `{key_1: value_1, key_2: value_2, ...}`
# MAGIC 
# MAGIC The keys and values can all be of any type. However, because each key maps to a value, it is important that *all keys are unique*.
# MAGIC 
# MAGIC Let's create a breakfast dictionary, where the keys are the type of food and the values are the number of those foods we ate for breakfast.

# COMMAND ----------

breakfast_dict = {"pancakes": 1, "eggs": 2, "waffles": 3}
breakfast_dict

# COMMAND ----------

# MAGIC %md <i18n value="14aa0189-5d2a-49f5-a497-b1a29839d03c"/>
# MAGIC 
# MAGIC 
# MAGIC #### Dictionary Methods
# MAGIC 
# MAGIC Dictionaries provide the method [**dict_object.get()**](https://www.w3schools.com/python/ref_dictionary_get.asp) to get the value in the dictionary for the given argument. 
# MAGIC 
# MAGIC Let's see how many waffles we ate.

# COMMAND ----------

breakfast_dict.get("waffles")

# COMMAND ----------

# MAGIC %md <i18n value="c8653cbc-95a1-474e-8675-bbef76473da7"/>
# MAGIC 
# MAGIC 
# MAGIC Alternatively, you can use the syntax **`dict_object[key]`**.

# COMMAND ----------

breakfast_dict["waffles"]

# COMMAND ----------

# MAGIC %md <i18n value="de5edaf0-5374-4f84-888d-3a2793cd30fe"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC You can update a dictionary similarly to a list by assigning **`breakfast_dict[key]`** to be something. 
# MAGIC 
# MAGIC If the key is present, it overwrites the current value. If not, it creates a new key-value pair. 
# MAGIC 
# MAGIC Let's say we ate another waffle, bringing our total up to 4 waffles, and then ate a yogurt.

# COMMAND ----------

print(breakfast_dict)
breakfast_dict["waffles"] += 1
breakfast_dict["yogurt"] = 1
print(breakfast_dict)

# COMMAND ----------

# MAGIC %md <i18n value="9492938e-d135-47f8-8256-804d70cc4ff8"/>
# MAGIC 
# MAGIC 
# MAGIC Notice the use of **`+=`** to increment the count of waffles.
# MAGIC 
# MAGIC **Question**: Why did we not use **`+=`** to increment the yogurt count?

# COMMAND ----------

# MAGIC %md <i18n value="8092da28-ccad-4265-a97e-fb30c6da249d"/>
# MAGIC 
# MAGIC 
# MAGIC In order to determine if a key is in a dictionary, we can use the method [**dict_name.keys()**](https://www.w3schools.com/python/ref_dictionary_keys.asp). This returns a list of the keys in the dictionary. 
# MAGIC 
# MAGIC Similar to lists, we can use **`in`** to see if our key is in the dictionary. Let's see if we ate bacon for breakfast.

# COMMAND ----------

print(breakfast_dict.keys())
print("bacon" in breakfast_dict.keys())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
