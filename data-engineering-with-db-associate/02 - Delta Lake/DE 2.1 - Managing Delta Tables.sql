-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="7aa87ebc-24dd-4b39-bb02-7c59fa083a14"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # Managing Delta Tables
-- MAGIC 
-- MAGIC If you know any flavor of SQL, you already have much of the knowledge you'll need to work effectively in the data lakehouse.
-- MAGIC 
-- MAGIC In this notebook, we'll explore basic manipulation of data and tables with SQL on Databricks.
-- MAGIC 
-- MAGIC Note that Delta Lake is the default format for all tables created with Databricks; if you've been running SQL statements on Databricks, you're likely already working with Delta Lake.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Create Delta Lake tables
-- MAGIC * Query data from Delta Lake tables
-- MAGIC * Insert, update, and delete records in Delta Lake tables
-- MAGIC * Write upsert statements with Delta Lake
-- MAGIC * Drop Delta Lake tables

-- COMMAND ----------

-- MAGIC %md <i18n value="add37b8c-6a95-423f-a09a-876e489ef17d"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- MAGIC %md <i18n value="3b9c0755-bf72-480e-a836-18a4eceb97d2"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Creating a Delta Table
-- MAGIC 
-- MAGIC There's not much code you need to write to create a table with Delta Lake. There are a number of ways to create Delta Lake tables that we'll see throughout the course. We'll begin with one of the easiest methods: registering an empty Delta Lake table.
-- MAGIC 
-- MAGIC We need: 
-- MAGIC - A **`CREATE TABLE`** statement
-- MAGIC - A table name (below we use **`students`**)
-- MAGIC - A schema
-- MAGIC 
-- MAGIC **NOTE:** In Databricks Runtime 8.0 and above, Delta Lake is the default format and you don’t need **`USING DELTA`**.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md <i18n value="a00174f3-bbcd-4ee3-af0e-b8d4ccb58481"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC If we try to go back and run that cell again...it will error out! This is expected - because the table exists already, we receive an error.
-- MAGIC 
-- MAGIC We can add in an additional argument, **`IF NOT EXISTS`** which checks if the table exists. This will overcome our error.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md <i18n value="408b1c71-b26b-43c0-b144-d5e92064a5ac"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Inserting Data
-- MAGIC Most often, data will be inserted to tables as the result of a query from another source.
-- MAGIC 
-- MAGIC However, just as in standard SQL, you can also insert values directly, as shown here.

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md <i18n value="853dd803-9f64-42d7-b5e8-5477ea61029e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC In the cell above, we completed three separate **`INSERT`** statements. Each of these is processed as a separate transaction with its own ACID guarantees. Most frequently, we'll insert many records in a single transaction.

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md <i18n value="7972982a-05be-46ce-954e-e9d29e3b7329"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Note that Databricks doesn't have a **`COMMIT`** keyword; transactions run as soon as they're executed, and commit as they succeed.

-- COMMAND ----------

-- MAGIC %md <i18n value="121bd36c-10c4-41fc-b730-2a6fb626c6af"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Querying a Delta Table
-- MAGIC 
-- MAGIC You probably won't be surprised that querying a Delta Lake table is as easy as using a standard **`SELECT`** statement.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="4ecaf351-d4a4-4803-8990-5864995287a4"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC What may surprise you is that Delta Lake guarantees that any read against a table will **always** return the most recent version of the table, and that you'll never encounter a state of deadlock due to ongoing operations.
-- MAGIC 
-- MAGIC To repeat: table reads can never conflict with other operations, and the newest version of your data is immediately available to all clients that can query your lakehouse. Because all transaction information is stored in cloud object storage alongside your data files, concurrent reads on Delta Lake tables is limited only by the hard limits of object storage on cloud vendors. (**NOTE**: It's not infinite, but it's at least thousands of reads per second.)

-- COMMAND ----------

-- MAGIC %md <i18n value="8a379d8d-7c48-43b0-8e25-3e653d8d6e86"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Updating Records
-- MAGIC 
-- MAGIC Updating records provides atomic guarantees as well: we perform a snapshot read of the current version of our table, find all fields that match our **`WHERE`** clause, and then apply the changes as described.
-- MAGIC 
-- MAGIC Below, we find all students that have a name starting with the letter **T** and add 1 to the number in their **`value`** column.

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md <i18n value="b307b3e7-5ed2-4df8-bdd5-6c25acfd072f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Query the table again to see these changes applied.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="d581b9a2-f450-43dc-bff3-2ea9cc46ad4c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Deleting Records
-- MAGIC 
-- MAGIC Deletes are also atomic, so there's no risk of only partially succeeding when removing data from your data lakehouse.
-- MAGIC 
-- MAGIC A **`DELETE`** statement can remove one or many records, but will always result in a single transaction.

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md <i18n value="b5b346b8-a3df-45f2-88a7-8cf8dea6d815"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Using Merge
-- MAGIC 
-- MAGIC Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command.
-- MAGIC 
-- MAGIC Databricks uses the **`MERGE`** keyword to perform this operation.
-- MAGIC 
-- MAGIC Consider the following temporary view, which contains 4 records that might be output by a Change Data Capture (CDC) feed.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fe009d5-513f-4b93-994f-1ae9a0f30a80"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Using the syntax we've seen so far, we could filter from this view by type to write 3 statements, one each to insert, update, and delete records. But this would result in 3 separate transactions; if any of these transactions were to fail, it might leave our data in an invalid state.
-- MAGIC 
-- MAGIC Instead, we combine these actions into a single atomic transaction, applying all 3 types of changes together.
-- MAGIC 
-- MAGIC **`MERGE`** statements must have at least one field to match on, and each **`WHEN MATCHED`** or **`WHEN NOT MATCHED`** clause can have any number of additional conditional statements.
-- MAGIC 
-- MAGIC Here, we match on our **`id`** field and then filter on the **`type`** field to appropriately update, delete, or insert our records.

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md <i18n value="77cee0a0-f94b-4016-a20b-08e4857d13db"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Note that only 3 records were impacted by our **`MERGE`** statement; one of the records in our updates table did not have a matching **`id`** in the students table but was marked as an **`update`**. Based on our custom logic, we ignored this record rather than inserting it. 
-- MAGIC 
-- MAGIC How would you modify the above statement to include unmatched records marked **`update`** in the final **`INSERT`** clause?

-- COMMAND ----------

-- MAGIC %md <i18n value="4eca2c53-e457-4964-875e-d39d9205c3c6"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Dropping a Table
-- MAGIC 
-- MAGIC Assuming that you have proper permissions on the target table, you can permanently delete data in the lakehouse using a **`DROP TABLE`** command.
-- MAGIC 
-- MAGIC **NOTE**: Later in the course, we'll discuss Table Access Control Lists (ACLs) and default permissions. In a properly configured lakehouse, users should **not** be able to delete production tables.

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md <i18n value="08cbbda5-96b2-4ae8-889f-b1f4c04d1496"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
