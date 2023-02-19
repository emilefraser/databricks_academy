# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="50d4a990-cefe-41dd-a439-12f7706751be"/>
# MAGIC 
# MAGIC 
# MAGIC # Classes
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC 
# MAGIC Apply concepts learned in the last lesson, including:
# MAGIC - Utilizing instance attributes to define data for new data types
# MAGIC - Using methods to add functionality to new data types

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="37e05756-08e7-4ec8-8583-0cf5d64eea2b"/>
# MAGIC 
# MAGIC 
# MAGIC ## Exercise: Simpson
# MAGIC 
# MAGIC <img src="https://i.pinimg.com/originals/13/63/37/13633734d116fe188af57fe9da7d095e.jpg" style="height:400">
# MAGIC 
# MAGIC Define a class **`Simpson`** representing members of the Simpson's family that has the following attributes and methods:
# MAGIC 
# MAGIC * Each Simpson has a **`first_name`**, **`age`**, and **`favorite_food`**. 
# MAGIC   * Define the **`__init__()`** method to initialize these attributes. 
# MAGIC   
# MAGIC * Define a method called **`simpson_summary()`** which returns a string in this format: `{first_name} Simpson is {age} years old and their favorite food is {favorite_food}`
# MAGIC   * For example, if `first_name="Homer"`, `age=39`, and `favorite_food="donuts"`, this should return: `Homer Simpson is 39 years old and their favorite food is donuts`
# MAGIC   
# MAGIC * Define a method called **`older()`** which takes in another **`Simpson`** object and returns **`True`** if the person that called the method is older than the other one, **`False`** otherwise.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md <i18n value="b3b3aa47-1699-4d9e-806d-fa84134c143f"/>
# MAGIC 
# MAGIC 
# MAGIC **Check your work:**

# COMMAND ----------

homer = Simpson("Homer", 39, "Donuts")
bart = Simpson("Bart", 10, "Hamburgers")
assert homer.first_name == "Homer", "first_name is not set properly"
assert homer.favorite_food == "Donuts", "favorite_food is not set properly"
assert bart.age == 10, "Age is not set properly"
assert homer.simpson_summary() == "Homer Simpson is 39 years old and their favorite food is Donuts", "simpson_summary is incorrect"
assert bart.simpson_summary() == "Bart Simpson is 10 years old and their favorite food is Hamburgers", "simpson_summary is incorrect"
assert homer.older(bart) == True, "Homer is older than Bart"
assert bart.older(homer) == False, "Bart is not older than Homer"
print("Test passed!")

# COMMAND ----------

# MAGIC %md <i18n value="TBD"/>
# MAGIC 
# MAGIC ### Bonus Exercise
# MAGIC 
# MAGIC - Create a class called Fraction consisting of the following:
# MAGIC   - Public attributes
# MAGIC     - Numerator
# MAGIC     - Denominator
# MAGIC   - Constructor that initializes the numerator and denominator
# MAGIC   - Method named __to_string__ that returns the Fraction as a string in the format
# MAGIC     Numerator / Denominator
# MAGIC   - Method named __as_decimal__ that returns the value of the fraction as a number (including decimal places)
# MAGIC   - Method named __invert__ that alters the state of the Fraction to it's inverted equivalent.
# MAGIC   - Method named __least_common_denominator__ that computes the leastCommonDenominator of 2 fractions.
# MAGIC   - A method named __reduce__ that reduces a Fraction to it's least common denominator
# MAGIC   - Method named __is_equal__ that compares a Fraction with another received in its argument list to determine whether or not they are equal
# MAGIC     - Note that 2/4 == 1/2
# MAGIC   - Method named __is_less_than__ that compares a Fraction with another received in its argument list to determine whether the object on which the method is invoked is smaller than the one received in its argument list
# MAGIC     - Note 2/4 is smaller than 5/9
# MAGIC   - A Method named __scale__ that scales a fraction to a designated multiplier
# MAGIC     - e.g. 1/4 scaled by 2 becomes 2/8
# MAGIC   - A method named __clone__ that given a Fraction in its argument list, alters its current state to that of the Fraction received in its arugment list

# COMMAND ----------

# I have provided a helper standalone function you may wish to invoke as part of your implementation

def greatest_common_factor(a, b):
    gcf = 1
    if a < b:
        limit = a
    else:
        limit = b
    
    for index in range(2, limit + 1):
        if (a % index == 0) and (b % index == 0):
            gcf = index
        
    return gcf

# COMMAND ----------

# TODO
class Fraction():

# COMMAND ----------

# Test your work
one_half = Fraction(1,2)
two_fourths = Fraction(2,4)
two_thirds = Fraction(2,3)
six_ninths = Fraction(6,9)

assert one_half.to_string() == "1 / 2", "to_string did not convert the string properly"
assert two_fourths.as_decimal() == .5, "as_decimal did not convert the fraction to a decimal number properly"

four_eighths = Fraction(4, 8)
four_eighths.invert()
assert four_eighths.to_string() == "8 / 4", "invert did not convert the fraction properly"

assert two_thirds.least_common_denominator(six_ninths) == 9.0, "least_common_denominator did not compute the least common denominator properly"

assert one_half.is_equal(two_fourths) == True, "is_equal did not compare two fractions properly"
assert one_half.is_less_than(two_thirds) == True, "is_less_than did not compare two fractions properly"

# COMMAND ----------

# MAGIC %md <i18n value="7799d859-cb3c-46ac-87e2-414d95de6fcf"/>
# MAGIC 
# MAGIC ### Bonus Exercise
# MAGIC 
# MAGIC Modify the aforementioned class implementation to properly encapsulate the numerator and denominator attributes so that they cannot be accessed directly from an application

# COMMAND ----------

# TODO
class Fraction():

# COMMAND ----------

# Test your work
one_half = Fraction(1,2)
two_fourths = Fraction(2,4)
two_thirds = Fraction(2,3)
six_ninths = Fraction(6,9)

assert one_half.to_string() == "1 / 2", "to_string did not convert the string properly"
assert two_fourths.as_decimal() == .5, "as_decimal did not convert the fraction to a decimal number properly"

four_eighths = Fraction(4, 8)
four_eighths.invert()
assert four_eighths.to_string() == "8 / 4", "invert did not convert the fraction properly"

assert two_thirds.least_common_denominator(six_ninths) == 9.0, "least_common_denominator did not compute the least common denominator properly"

assert one_half.is_equal(two_fourths) == True, "is_equal did not compare two fractions properly"
assert one_half.is_less_than(two_thirds) == True, "is_less_than did not compare two fractions properly"

nine_fifteenths = Fraction(9, 15)
nine_fifteenths.reduce()
assert nine_fifteenths.is_equal(Fraction(3, 5)), "The reduce function did not reduce its fraction properly"

assert six_ninths.get_numerator() == 6, f"The numerator for {sixNinths.to_string()} has been modified"

try:
    six_ninths.numerator 
    six_ninths.denominator
    print("The fraction has not been encapsulated properly")
except Exception as ex:
    pass

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
