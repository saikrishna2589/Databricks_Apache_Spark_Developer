# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Orders Data - Explode Arrays
# MAGIC
# MAGIC 1. Access elements from JSON objects
# MAGIC 2. Depulicate Array Elements
# MAGIC 3. Explode Arrays
# MAGIC 4. Write the transformed data to the silver schema
# MAGIC

# COMMAND ----------

df = spark.read.table("gizmobox.silver.orders")

display(df)

# COMMAND ----------

# %sql
# SELECT *
# FROM gizmobox.silver.orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Access elements from JSON object ('.' notation)

# COMMAND ----------

# DBTITLE 1,Untitled
# %sql
# SELECT 
# json_object_value.customer_id,
# json_object_value.items,
# json_object_value.order_date :: DATE AS order_date,
# json_object_value.order_id,
# json_object_value.order_status,
# json_object_value.payment_method,
# json_object_value.total_amount,
# json_object_value.transaction_timestamp::TIMESTAMP  AS transaction_timestamp
# FROM gizmobox.silver.orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Deduplicate the Array Elements (array_distinct() function)

# COMMAND ----------

from pyspark.sql.functions import col,array_distinct,to_date,to_timestamp

df_deduplicated_array = df.select(col("json_object_value.customer_id"),\
                                  array_distinct(col("json_object_value.items")).alias("items"),\
                                      to_date(col("json_object_value.order_date"),"yyyy-MM-dd").alias("order_date"),\
                                         col("json_object_value.order_id"),col("json_object_value.order_status"),\
                                         col("json_object_value.payment_method"),col("json_object_value.total_amount"),\
                                         to_timestamp(col("json_object_value.transaction_timestamp"),"yyyy-MM-dd HH:mm:SS").alias("transaction_timestamp")
                                  
                                  )
                                  

# COMMAND ----------

display(df_deduplicated_array)

# COMMAND ----------

# DBTITLE 1,Untitled
# %sql

#  SELECT 
# json_object_value.customer_id,
# array_distinct(json_object_value.items),
# json_object_value.order_date :: DATE AS order_date,
# json_object_value.order_id,
# json_object_value.order_status,
# json_object_value.payment_method,
# json_object_value.total_amount,
# json_object_value.transaction_timestamp::TIMESTAMP  AS transaction_timestamp
# FROM gizmobox.silver.orders


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Exploding an array so each object of the array has its own row.

# COMMAND ----------

from pyspark.sql.functions import explode
dfdeduped_explode_array = df_deduplicated_array.select(col("customer_id"),\
                                     explode(col("items")).alias("exploded_items"),\
                                      col("order_date"),
                                         col("order_id"),col("order_status"),\
                                         col("payment_method"),col("total_amount"),\
                                         to_timestamp(col("transaction_timestamp")).alias("transaction_timestamp"))



display(dfdeduped_explode_array)
                      

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMP VIEW 
# tv_orders_explode
# AS 
#     SELECT 
#     json_object_value.customer_id,
#     explode(array_distinct(json_object_value.items)) AS item,
#     json_object_value.order_date :: DATE AS order_date,
#     json_object_value.order_id,
#     json_object_value.order_status,
#     json_object_value.payment_method,
#     json_object_value.total_amount,
#     json_object_value.transaction_timestamp::TIMESTAMP  AS transaction_timestamp
#     FROM gizmobox.silver.orders


# COMMAND ----------

# %sql
# SELECT * FROM tv_orders_explode

# COMMAND ----------


# --Extracting the json object futher to have each element in its own column(dot notation)

dfdeduped_explode_array_column_extract = dfdeduped_explode_array.select(col("customer_id"),col("exploded_items.category"),\
col("exploded_items.details.brand"),\
col("exploded_items.details.color"),\
col("exploded_items.item_id"),\
col("exploded_items.name"),\
col("exploded_items.price"),\
col("exploded_items.quantity"),\
col("order_date"),\
col("order_id"),\
col("order_status"),\
col("payment_method"),\
col("total_amount"),\
col("transaction_timestamp"))

# COMMAND ----------

display(dfdeduped_explode_array_column_extract)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write the Transformed Data to silver schema

# COMMAND ----------

#so we did
# remove duplicates from arrays using array_distinct function on array (item column)
# explode the array column using explode function that adds new row for each dictionary in the item list column for order details. other rows such as column id are duplicated
#access elements now in this column using dot notation.
# save the data into silver table
dfdeduped_explode_array_column_extract.writeTo('gizmobox.silver.py_orders_Main').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_orders_main

# COMMAND ----------

# %sql
# --Extracting the json object futher to have each element in its own column(dot notation)
# CREATE TABLE IF NOT EXISTS gizmobox.silver.orders_main
# SELECT customer_id,
# item.category, 
# item.details.brand,
# item.details.color,
# item.item_id,
# item.name,
# item.price,
# item.quantity,
# order_date,
# order_id,
# order_status,
# payment_method,
# total_amount,
# transaction_timestamp
# FROM tv_orders_explode

# COMMAND ----------

# %sql
# SELECT * FROM gizmobox.silver.orders_main
