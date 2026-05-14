# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Profiling in DataBricks
# MAGIC 1. Data Profiling using UI
# MAGIC 2. using DBUTILS package (dbutils.data.summarize method)
# MAGIC 3. profiling using manual method
# MAGIC       - COUNT
# MAGIC       - COUNTIF
# MAGIC       - MIN
# MAGIC       - MAX 
# MAGIC       - WHERE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM samples.nyctaxi.trips
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Data Profiling using DBUTILS package(dbutils.data.summarise())
# MAGIC - we need to pass dataframe into this function.

# COMMAND ----------

df = spark.table('samples.nyctaxi.trips')
display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Using manual methods like aggregates for data profiling

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM 
# MAGIC gizmobox.bronze.v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Count how many records of customer id are null
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT_IF(customer_id IS NULL) AS `null_customerids(primary_key)`
# MAGIC FROM gizmobox.bronze.v_customers

# COMMAND ----------


