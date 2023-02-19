# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="3fb78239-dd9c-4c5d-b4dc-6049604db4b2"/>
# MAGIC 
# MAGIC 
# MAGIC # Classes
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC - Explore how to define a new data called a class
# MAGIC - Utilize instance attributes to define data for classes
# MAGIC - Use methods to add functionality to classes

# COMMAND ----------

# MAGIC %md <i18n value="f8f3096e-b29e-4c8e-8874-96451f47002a"/>
# MAGIC 
# MAGIC 
# MAGIC ## Classes
# MAGIC 
# MAGIC From [W3Schools](https://www.w3schools.com/python/python_classes.asp): 
# MAGIC ```
# MAGIC Python is an object oriented programming language. Almost everything in Python is an object, with its properties and methods. 
# MAGIC 
# MAGIC A Class is like an object constructor, or a "blueprint" for creating objects.
# MAGIC ```
# MAGIC 
# MAGIC When we worked with functions, they allowed us to reuse the same code applied to different parameters. **Classes can be thought of as a step beyond functions as they provide a reusable blueprint for both code and data.**
# MAGIC 
# MAGIC We've now seen the basic built-in types and some more advanced collection types. We can also define our own custom [**classes**](https://www.w3schools.com/python/python_classes.asp) to fit our needs. 
# MAGIC 
# MAGIC To define a class we write:
# MAGIC 
# MAGIC ```
# MAGIC class ClassName():
# MAGIC     <code block>
# MAGIC ```
# MAGIC 
# MAGIC Up until now, we've typically used the `snake_case` convention. However, when defining a class, [Python style guides](https://peps.python.org/pep-0008/) recommend using `CapWords`, with every word capitalized and no spaces. 
# MAGIC 
# MAGIC **Note**: **`pass`** tells Python to not do anything. We're effectively defining a dog that does nothing (not quite what you'd want in a dog!).

# COMMAND ----------

# Create the blue print
class Dog():
    pass

# COMMAND ----------

# MAGIC %md <i18n value="b326ce06-9f66-46f3-a134-03579d30867a"/>
# MAGIC 
# MAGIC 
# MAGIC For the built-in types we have seen so far, creating an object from a class has been built-in. If you write **`1`** or **`"hello"`** Python knows what types those are and creates those `int` or `str` objects for you. 
# MAGIC 
# MAGIC However, for classes that we define we have to create objects as follows:
# MAGIC 
# MAGIC **`object_name = ClassName()`**
# MAGIC 
# MAGIC Technically, this is called "instantiating" the class as we create a specific version of the object. Now we have a **`Dog`** class. Let's make a **`Dog`** object, and call it **`my_dog`**.

# COMMAND ----------

# Instantiate the blueprint and save it to the variable
my_dog = Dog()

type(my_dog)

# COMMAND ----------

# MAGIC %md <i18n value="58c970b2-4bd1-4b0b-8533-b0794b07eed3"/>
# MAGIC 
# MAGIC 
# MAGIC ## Code Reuse with Methods
# MAGIC 
# MAGIC Now that we can make a **`Dog`** that does nothing, let's add some functionality!
# MAGIC 
# MAGIC Functionality in classes are defined by [**methods**](https://www.w3schools.com/python/gloss_python_object_methods.asp), which we saw in a previous lesson.
# MAGIC 
# MAGIC As a reminder, a **method** is a special function that acts on an object that we call like this: **`object.method(args)`**
# MAGIC 
# MAGIC We define a method similarly to how we define a normal function except with two differences:
# MAGIC 
# MAGIC 1. We nest the definition of the method within the class definition. 
# MAGIC 1. We must specify a parameter named **`self`**, followed by any additional parameters.
# MAGIC 
# MAGIC This looks like this:
# MAGIC 
# MAGIC ```
# MAGIC class ClassName():
# MAGIC 
# MAGIC     def method_name(self, args):
# MAGIC         method code
# MAGIC ```
# MAGIC 
# MAGIC We will ignore the **`self`** parameter for now, but come back to it in a moment.
# MAGIC 
# MAGIC Let's look at a very simple example: writing a method that takes in a name and returns it.

# COMMAND ----------

class UpdatedDog():
    
    def return_name(self, name):
        return f"name: {name}"

# COMMAND ----------

# MAGIC %md <i18n value="5de3ea33-9f20-4ca7-913c-f08dba3ba9ca"/>
# MAGIC 
# MAGIC 
# MAGIC Now let's make an object of our updated **`Dog`** class and call the method. Remember we call methods like this: **`object.method(args)`**.
# MAGIC 
# MAGIC Note that we do *not* pass an argument for the special **`self`** parameter.

# COMMAND ----------

my_updated_dog = UpdatedDog()

my_updated_dog.return_name("Rex")

# COMMAND ----------

# MAGIC %md <i18n value="1dd48eff-d93e-4f8e-9373-ae10de48af4d"/>
# MAGIC 
# MAGIC 
# MAGIC What about `self`?
# MAGIC 
# MAGIC A method differs from a function in that it can act on the object it's called on. The method needs to be able to reference the object that called it. That's what **`self`** refers to.
# MAGIC 
# MAGIC When we call **`object.method(self, args)`** the object itself is passed to the **`self`** parameter automatically by Python.

# COMMAND ----------

class DogWithSelf():
    
    def print_self(self):
        print(self)
        
dog_with_self = DogWithSelf()

print(dog_with_self)
dog_with_self.print_self() # prints the same object

# COMMAND ----------

# MAGIC %md <i18n value="56944397-f05e-4cdd-bced-940be45e1872"/>
# MAGIC 
# MAGIC 
# MAGIC Notice that when we print the **`new_dog`** object and call our **`print_self()`** method on **`new_dog`** we see the same object. 
# MAGIC 
# MAGIC That's because **`new_dog`** was passed as the argument to **`self`** in **`print_self()`**

# COMMAND ----------

# MAGIC %md <i18n value="a3aea935-bc40-4fc2-aeb0-f3d5d7c45eb2"/>
# MAGIC 
# MAGIC 
# MAGIC ## Data Caching with Attributes
# MAGIC 
# MAGIC In our class, we need some way to store data. **When a variable is stored in a class, it is called an attribute.** Attributes are just variables that are defined for each instance of an object. Every instance will have the same named attributes but they're normally set to different values.
# MAGIC 
# MAGIC ```
# MAGIC class ClassName():
# MAGIC 
# MAGIC     def __init__(self, arg):
# MAGIC         self.arg = arg
# MAGIC ```
# MAGIC 
# MAGIC Python provides a special method called **`__init__(self)`** that is automatically called when our object is initialized. This is often referred to as the *constructor method* for the class since it constructs the class's attributes.
# MAGIC 
# MAGIC Let's say for our **`Dog`** class that we want every dog object to have a name and a color. To do that, we create two attributes for the class **`name`** and **`color`**.
# MAGIC 
# MAGIC Then, every **`Dog`** object has a name and color attribute, but they can be set to different values for each object, so that each dog object can have its own name and color.

# COMMAND ----------

class DogWithAttributes():

    def __init__(self, name, color):
        print("This ran automatically!")
        self.name = name
        self.color = color

dog_with_attributes = DogWithAttributes("Rex", "Orange")

# COMMAND ----------

# MAGIC %md <i18n value="a6ca39f1-ceb0-42ff-aa95-fac34ef7ba1b"/>
# MAGIC 
# MAGIC 
# MAGIC When the `__init__()` method was automatically called, it saved those variables to `self`, which is the instantiation of the object. We can access the attribute similar to how we accessed methods but without the parentheses.

# COMMAND ----------

dog_with_attributes.name

# COMMAND ----------

# MAGIC %md <i18n value="3d8f5672-e9d4-4bd4-9c14-91756c1cbb4f"/>
# MAGIC 
# MAGIC 
# MAGIC In a method definition we can access an attribute using **`self.attribute_name`**, since **`self`** refers to the object that calls the method, regardless of what we named it when we instantiated it.

# COMMAND ----------

class DogWithAttributesAndMethod():
    
    def __init__(self, name, color):
        self.name = name
        self.color = color
        
    def return_name(self):
        return self.name
        
my_dog = DogWithAttributesAndMethod("Rex", "Blue")
my_dog.return_name()

# COMMAND ----------

# MAGIC %md <i18n value="4ec0fb49-f058-4c68-be13-aac66e9e5fe3"/>
# MAGIC 
# MAGIC 
# MAGIC We now have all the tools we need to add functionality! Let's say we want to add the ability to change a dog's name. We can simply update the **`name`** attribute like this.

# COMMAND ----------

class DogWithAttributesAndMethods():
    
    def __init__(self, name, color):
        self.name = name
        self.color = color
        
    def return_name(self):
        return self.name
        
    def update_name(self, new_name):
        self.name = new_name
        
my_dog = DogWithAttributesAndMethods("Rex", "Blue")
print(f"Here's my name now: {my_dog.return_name()}")

my_dog.update_name("Brady")
print(f"Here's my name after updating it: {my_dog.return_name()}")

# COMMAND ----------

# MAGIC %md <i18n value="f781f535-f48d-4fb6-a110-c52e5a6c4039"/>
# MAGIC 
# MAGIC 
# MAGIC ## More Advanced Classes 
# MAGIC 
# MAGIC Classes can have many methods and attributes. They can also access the attributes of another class.
# MAGIC 
# MAGIC Take a look at the `return_both_names` method to see how a class can use the attributes of another class.

# COMMAND ----------

class DogFinal():
    
    def __init__(self, name_str, color_str):
        self.name = name_str
        self.color = color_str
        
    def return_name(self):
        return self.name
        
    def update_name(self, new_name):
        self.name = new_name
        
    def return_both_names(self, other_dog_object):
        return self.name + " and " + other_dog_object.name
        
dog_1 = DogFinal("Rex", "Blue")
dog_2 = DogFinal("Brady", "Purple")

dog_1.return_both_names(dog_2)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
