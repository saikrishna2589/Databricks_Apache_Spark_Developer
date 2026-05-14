# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Address Data
# MAGIC 1. Create one record for each customer with 2 columns for addresses instead of currently : 
# MAGIC **1 for shipping and 1 for billing address**
# MAGIC 2. Write transformed data to the silver schema.

# COMMAND ----------


df= spark.read.table("gizmobox.bronze.v_addresses")

display(df)

# COMMAND ----------

# %sql
# SELECT 
#     customer_id,
#     address_type,
#     address_line_1,
#     city,
#     state,
#     postcode
# FROM gizmobox.bronze.v_addresses;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create one record for each customer with 2 columns for addresses instead of currently : 
# MAGIC **1 for shipping and 1 for billing address**
# MAGIC
# MAGIC **Use PIVOT statement** --> unique rows per customer_id into columns (so wide rather than long)

# COMMAND ----------

# DBTITLE 1,Untitled

from pyspark.sql.functions import max

df_pivot = df.groupBy('customer_id')\
            .pivot('address_type' ,['shipping','billing'] )\
            .agg(max('address_line_1').alias('address_line_1'),max('city').alias('city'),\
             max('state').alias('state'), max('postcode').alias('postcode')).orderBy('customer_id')

# COMMAND ----------

display(df_pivot)

# COMMAND ----------

df_pivot.writeTo('gizmobox.silver.py_addresses').createOrReplace()

# COMMAND ----------

df_pivot_import = spark.read.table('gizmobox.silver.py_addresses')
display(df_pivot_import)

# COMMAND ----------

# %sql
# SELECT *
# FROM 
#     (SELECT 
#         customer_id,
#         address_type,
#         address_line_1,
#         city,
#         state,
#         postcode
#     FROM gizmobox.bronze.v_addresses)

#     PIVOT(
#       MAX(address_line_1) AS address_line,
#       MAX(city) AS city,
#       MAX(state) AS state,
#       MAX(postcode) AS postcode
#       FOR address_type in ('shipping', 'billing')
#       )
#   ORDER BY customer_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. #### Save the data into Silver table 

# COMMAND ----------

df

# COMMAND ----------

# %sql
# CREATE TABLE gizmobox.silver.addresses
# AS
# SELECT *
# FROM 
#     (SELECT 
#         customer_id,
#         address_type,
#         address_line_1,
#         city,
#         state,
#         postcode
#     FROM gizmobox.bronze.v_addresses)

#     PIVOT(
#       MAX(address_line_1) AS address_line,
#       MAX(city) AS city,
#       MAX(state) AS state,
#       MAX(postcode) AS postcode
#       FOR address_type in ('shipping', 'billing')
#       )
#   ORDER BY customer_id DESC

# COMMAND ----------

# %sql
# SELECT * FROM gizmobox.silver.addresses

# COMMAND ----------


