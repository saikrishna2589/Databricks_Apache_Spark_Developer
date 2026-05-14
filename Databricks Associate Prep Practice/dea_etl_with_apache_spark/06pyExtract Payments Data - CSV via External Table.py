# Databricks notebook source
# MAGIC %md
# MAGIC #### Create External Table using Payments Data
# MAGIC 1. List the files from Payment folder
# MAGIC 2.Provide schema to the dataframe as no headers in source data
# MAGIC 3. write data into bronze layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.List the files from Payment folder

# COMMAND ----------

# MAGIC %fs ls 'abfss://gizmobox@dbccourseextdl.dfs.core.windows.net/landing/external_data/payments'

# COMMAND ----------

# %sql
# SELECT *
# FROM 
# CSV.`abfss://gizmobox@dbccourseextdl.dfs.core.windows.net/landing/external_data/payments`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Provide Schema 1st method (StructType StructField)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType,StringType

# COMMAND ----------

# DBTITLE 1,Untitled
schema = StructType([
                StructField("payment_id", IntegerType(),True),
                StructField("order_id", IntegerType(),False),
                   StructField("payment_timestamp", TimestampType(),False),
                        StructField("payment_status", IntegerType(),False),
                        StructField("payment_method", StringType(),False)])

# COMMAND ----------

df = spark.read.format('csv').schema(schema).load('abfss://gizmobox@dbccourseextdl.dfs.core.windows.net/landing/external_data/payments')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2nd method to declare the data schema

# COMMAND ----------

schema_ddl_method = "payment_id INTEGER, order_id INTEGER,payment_timestamp TIMESTAMP,payment_status INTEGER,payment_method STRING"

df_ddl_method =spark.read.format("csv")\
.schema(schema_ddl_method)\
.option("sep",',')\
    .load('abfss://gizmobox@dbccourseextdl.dfs.core.windows.net/landing/external_data/payments')


# COMMAND ----------

display(df_ddl_method)

# COMMAND ----------

# %md
# #### 2. Create External Table

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS gizmobox.bronze.payments
# (payment_id INTEGER,
# order_id INTEGER,
# payment_timestamp TIMESTAMP,
# payment_status INTEGER,
# payment_method STRING
# )
# USING CSV
# OPTIONS
#  (
#    header = True,
#    delimiter =','
# )
# LOCATION 'abfss://gizmobox@dbccourseextdl.dfs.core.windows.net/landing/external_data/payments'

# COMMAND ----------

# MAGIC %md
# MAGIC #### write the df into bronze layer

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_payments').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM  
# MAGIC gizmobox.bronze.py_payments

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Demonstrate the effect of adding/updating/deleting files.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC when data in storage account gets changed like file gets deleted, added or removed, this info will not flow through UC metadata automatically. so you will still need to refresh hte metadata first , so the most up to date data changes are captured in SELECT statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE gizmobox.bronze.payments

# COMMAND ----------

# %sql
# SELECT * 
# FROM  
# gizmobox.bronze.payments

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Effect of Dropping the Table

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS gizmobox.bronze.payments

# --only the table in UC metadta will be deleted . all the underlying sata in storage remains as is for external table.
