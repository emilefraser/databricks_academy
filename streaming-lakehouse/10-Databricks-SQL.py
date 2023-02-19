# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks SQL
# MAGIC 
# MAGIC This notebook is intended for an instructor-led demonstration of Databricks SQL. It re-uses the data and approach of the previous notebook on MLflow.
# MAGIC 
# MAGIC The setup script here clears out all databases, trains models, starts both streaming predictions, and loads three batches of data.

# COMMAND ----------

# MAGIC %run ./Includes/setup-sqla

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below can be used to trigger new batches of data.

# COMMAND ----------

File.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to shut down all streams before leaving the demo.

# COMMAND ----------

# for stream in spark.streams.active:
#     stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC All the queries used in the dashboard demo are copied below for reference.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW airbnb_demo_db.v_error_metrics AS (
# MAGIC     SELECT id, a.batch_date, price-y_avg baseline_error, price-predicted error, POWER(price-y_avg, 2) baseline_2, POWER(price-predicted, 2) squared_error, price, predicted
# MAGIC     FROM airbnb_demo_db.preds_production a
# MAGIC     LEFT JOIN (    
# MAGIC         SELECT batch_date, AVG(price) y_avg
# MAGIC         FROM airbnb_demo_db.preds_production
# MAGIC         GROUP BY batch_date) b
# MAGIC     ON a.batch_date = b.batch_date
# MAGIC );
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW airbnb_demo_db.v_model_performance AS (
# MAGIC     SELECT batch_date, 
# MAGIC         1-(SUM(squared_error)/SUM(baseline_2)) r2, 
# MAGIC         SQRT(AVG(squared_error)) rmse, 
# MAGIC         SQRT(AVG(baseline_2)) baseline_rmse, 
# MAGIC         SUM(squared_error) sse, 
# MAGIC         SUM(baseline_2) baseline_error
# MAGIC     FROM airbnb_demo_db.v_error_metrics
# MAGIC     GROUP BY batch_date
# MAGIC     ORDER BY batch_date ASC
# MAGIC );
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW airbnb_demo_db.v_error_metrics_staging AS (
# MAGIC     SELECT id, a.batch_date, price-y_avg baseline_error, price-predicted error, POWER(price-y_avg, 2) baseline_2, POWER(price-predicted, 2) squared_error, price, predicted
# MAGIC     FROM airbnb_demo_db.preds_staging a
# MAGIC     LEFT JOIN (    
# MAGIC         SELECT batch_date, AVG(price) y_avg
# MAGIC         FROM airbnb_demo_db.preds_staging
# MAGIC         GROUP BY batch_date) b
# MAGIC     ON a.batch_date = b.batch_date
# MAGIC );
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW airbnb_demo_db.v_model_performance_staging AS (
# MAGIC     SELECT batch_date, 
# MAGIC         1-(SUM(squared_error)/SUM(baseline_2)) r2, 
# MAGIC         SQRT(AVG(squared_error)) rmse, 
# MAGIC         SQRT(AVG(baseline_2)) baseline_rmse, 
# MAGIC         SUM(squared_error) sse, 
# MAGIC         SUM(baseline_2) baseline_error
# MAGIC     FROM airbnb_demo_db.v_error_metrics_staging
# MAGIC     GROUP BY batch_date
# MAGIC     ORDER BY batch_date ASC
# MAGIC );
# MAGIC 
# MAGIC SELECT DISTINCT(batch_date) 
# MAGIC FROM airbnb_demo_db.silver_chicago;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id, a.price, predicted, accommodates, beds, minimum_nights, maximum_nights, number_of_reviews, review_scores_rating, days_since_last_review
# MAGIC FROM airbnb_demo_db.silver_chicago a
# MAGIC INNER JOIN airbnb_demo_db.v_error_metrics b
# MAGIC ON a.id = b.id AND a.batch_date = b.batch_date
# MAGIC WHERE a.batch_date = (SELECT MAX(batch_date) FROM airbnb_demo_db.silver_chicago)
# MAGIC ORDER BY b.error DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM airbnb_demo_db.v_model_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.batch_date, a.r2 r2_prod, b.r2 r2_staging, a.rmse rmse_prod, b.rmse rmse_staging
# MAGIC FROM airbnb_demo_db.v_model_performance a
# MAGIC INNER JOIN airbnb_demo_db.v_model_performance_staging b
# MAGIC ON a.batch_date = b.batch_date
# MAGIC ORDER BY a.batch_date ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price, predicted, error
# MAGIC FROM airbnb_demo_db.v_error_metrics
# MAGIC WHERE batch_date = (SELECT MAX(batch_date) FROM airbnb_demo_db.silver_chicago)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, price, predicted, error
# MAGIC FROM airbnb_demo_db.v_error_metrics_staging
# MAGIC WHERE batch_date = (SELECT MAX(batch_date) FROM airbnb_demo_db.silver_chicago)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
