# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data from Orders JSON file
# MAGIC 1. Query orders file using JSON format
# MAGIC 2. Query orders file using TEXT format
# MAGIC 3. Create orders View in bronze Schema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Query Orders File using JSON format

# COMMAND ----------

# MAGIC  %sql
# MAGIC SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC USE CATALOG gizmobox
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_SCHEMA();
# MAGIC --USE SCHEMA landing

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data using Pyspark

# COMMAND ----------

df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/orders')

display(df)

# COMMAND ----------

# %sql

# --corrupt records as json is unable to parse correctly all columns .so we will read as text format as store in view in broze as is without parsing for now.
# --later we will pre-process then parse as json and them store into silver layer .for now we store in bronze unprocessed . see next step for this.
# SELECT * 
# FROM 
# JSON.`/Volumes/gizmobox/landing/operational_data/orders`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 2. Query orders file in text format  

# COMMAND ----------

df_text = spark.read.format('text').load('/Volumes/gizmobox/landing/operational_data/orders')
display(df_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC TEXT.`/Volumes/gizmobox/landing/operational_data/orders`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Creating orders view in bronze schema

# COMMAND ----------

#df_text.write.format('delta').saveAsTable('path')
df_text.writeTo('gizmobox.bronze.py_orders').createOrReplace()

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW gizmobox.bronze.v_orders
# AS
# SELECT * 
# FROM 
# TEXT.`/Volumes/gizmobox/landing/operational_data/orders`


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_orders

# COMMAND ----------


