# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Orders Data - String to JSON
# MAGIC 1. Pre-process the JSON string to fix the data quality issues.
# MAGIC 2. Transform JSON string to JSON object
# MAGIC 3. Write the transformed data to the silver schema
# MAGIC

# COMMAND ----------

df = spark.read.table("gizmobox.bronze.v_orders")
display(df)

# COMMAND ----------

# %sql
# SELECT * 
# FROM gizmobox.bronze.v_orders


# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Pre-process the JSON string to fix the data quality issues.

# COMMAND ----------

#get rows where there are no quotes in order date

df_rlike_data_issue_rows = df.filter(df['value'].rlike('"order_date"\\s*:\\s*\\d{4}-\\d{2}-\\d{2}'))

display(df_rlike_data_issue_rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC --pick up rows where there are no quotes for the values of order_date key. we will need to add quotes to this to make it string format 
# MAGIC
# MAGIC -- SELECT value
# MAGIC -- FROM gizmobox.bronze.v_orders
# MAGIC -- WHERE value RLIKE '"order_date"\\s*:\\s*\\d{4}-\\d{2}-\\d{2}'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.Using regexp_replace to identify the pattern and replace with quotes in the 3rd bracket that is the value of order_date

# COMMAND ----------

#get rows where there are no quotes in order date
from pyspark.sql.functions import col,regexp_replace
df_json_regexp_replace = df.select(\
    regexp_replace(\
        col('value'),\
            '("order_date"\\s*):(\\s*)(\\d{4}-\\d{2}-\\d{2})',\
                '$1:$2:"$3"').alias('value_fixed'))


# COMMAND ----------

# display(df_json_regexp_replace)

# COMMAND ----------

# %sql
# SELECT value, regexp_replace(value,
#                              '("order_date"\\s*):(\\s*)(\\d{4}-\\d{2}-\\d{2})' ,
#                             '$1:$2"$3"' ) AS value_fixed

# FROM gizmobox.bronze.v_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. checking to make sure all data is fixed and quotes added so filter on no quotes returns null

# COMMAND ----------

# check_orderdate_quotes_left = df_json_regexp_replace.filter(col('value_fixed')\
#     .rlike(r'"order_date"\\s*:\\s*\\d{4}-\\d{2}-\\d{2}'))

# COMMAND ----------

display(check_orderdate_quotes_left)

# COMMAND ----------

# DBTITLE 1,Untitled
# %sql

# WITH replace_quotes_in_order_date AS 
# ( SELECT value, regexp_replace(value,
#                              '("order_date"\\s*):(\\s*)(\\d{4}-\\d{2}-\\d{2})' ,
#                             '$1:$2"$3"' ) AS value_fixed

# FROM gizmobox.bronze.v_orders)

# SELECT value,value_fixed
# FROM replace_quotes_in_order_date
# WHERE value_fixed RLIKE '"order_date"\\s*:\\s*\\d{4}-\\d{2}-\\d{2}'


                  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Creating a temp view on this regex fixed value

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMP VIEW tv_orders_fixed
# AS
# SELECT value, regexp_replace(value,
#                              '("order_date"\\s*):\\s*(\\d{4}-\\d{2}-\\d{2})' ,
#                             '$1:"$2"' ) AS value_fixed

# FROM gizmobox.bronze.v_orders

# COMMAND ----------

# %sql
#   SELECT * FROM tv_orders_fixed

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 4. Transform the JSON string into JSON object

# COMMAND ----------

# MAGIC %md 
# MAGIC - Function : _schema_of_json_
# MAGIC - Function : _from_json_

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import schema_of_json,col



function_strcut_output =df_json_regexp_replace.select(schema_of_json(col('value_fixed')).alias('schema_of_json_column'))

display(function_strcut_output.limit(1))

# COMMAND ----------

schema ='STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>'

# COMMAND ----------

from pyspark.sql.functions import col, from_json

df_json_object = df_json_regexp_replace.select(from_json(col('value_fixed'), schema).alias('json_object_data'))

# COMMAND ----------

display(df_json_object)

# COMMAND ----------

#save the data into silver layer

df_json_object.writeTo('gizmobox.silver.py_orders_json').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_orders_json

# COMMAND ----------

display(df_json_object.select(col("json_object_data.customer_id")))

# COMMAND ----------

df_json_struct_unpacked= df_json_object.select(col('json_object_data.*'))

# COMMAND ----------

from pyspark.sql.functions import explode
display(df_json_struct_unpacked.select(col("order_id"), explode(col("items")).alias('items')))

# COMMAND ----------

# %sql
# --get the schema of the json string by passing the row object to schema_of_json function.
# --you get STRUCT object in return. pass this schema into from_json() function to convert json string to json object.

# SELECT schema_of_json('{"order_id": 1, "customer_id": 6973, "order_date": "2025-01-05", "transaction_timestamp": "2025-01-05 10:13:59", "total_amount": 499, "payment_method": "Bank Transfer", "items": [{"item_id": 8, "name": "Gaming Console", "category": "Electronics", "price": 499, "quantity": 1, "details": {"brand": "Sony", "color": "Blue"}}], "order_status": "Completed"}')


# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMP VIEW json_string_to_object 
# AS

# SELECT from_json(value_fixed,'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_object_value
# FROM tv_orders_fixed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM json_string_to_object

# COMMAND ----------

# %sql
# select json_object_value.items[0].category,
# json_object_value.total_amount,
# json_object_value.payment_method
# FROM json_string_to_object

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gizmobox.silver.orders
# MAGIC AS
# MAGIC
# MAGIC SELECT  from_json(value_fixed,'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') AS json_object_value
# MAGIC FROM tv_orders_fixed
