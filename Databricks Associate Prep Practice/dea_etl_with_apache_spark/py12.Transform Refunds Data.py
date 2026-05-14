# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Refunds Data
# MAGIC - Extract refund reason and refund source from refund_reason combined column using SPLIT function
# MAGIC - Extract refund reason and refund source from refund_reason combined column using REGEXP_EXTRACT function
# MAGIC - Extract data and time from refund_timestamp column
# MAGIC - Save data to silver schema

# COMMAND ----------


df = spark.read.table("hive_metastore.bronze.refunds")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Untitled
#split the column refund_reason using SPLIT function
from pyspark.sql.functions import split, col,date_format

split_column = split(df["refund_reason"],":") #this will create output of array of string elements,so you can extract the elements using .getItem method.


df_split_column_data = df.select(\
                  col("refund_id"),\
                    col("payment_id"),\
                  date_format(col("refund_timestamp"),"yyyy-MM-dd").cast('date').alias("refund_date"),\
                  date_format(col("refund_timestamp"),"HH-mm-ss a").alias("refund_time"),
                  col("refund_amount"),\
                  split_column.getItem(0).alias("refund_reason")\
                    ,split_column.getItem(1).alias("refund_source"))

display(df_split_column_data )


#df_split_function = df.select(split(col("refund_reason"),":"))

# COMMAND ----------

# %sql
# SELECT refund_id,
# payment_id,
# refund_timestamp,
# refund_amount,
# refund_reason,
# SPLIT(refund_reason, ':')[0] AS refund_reason, --from array choose first element
# SPLIT(refund_reason, ':')[1] AS refund_source --from arry choose 2nd element
# FROM hive_metastore.bronze.refunds

# COMMAND ----------

#split data column using regexp_extract function in python

from pyspark.sql.functions import regexp_extract

df_regexp_extract_function = df.select(\
                  col("refund_id"),\
                    col("payment_id"),\
                  date_format(col("refund_timestamp"),"yyyy-MM-dd").cast('date').alias("refund_date"),\
                  date_format(col("refund_timestamp"),"HH-mm-ss a").alias("refund_time"),
                  col("refund_amount"),\
                      regexp_extract(col("refund_reason"), "^([^:]+):(.+$)",1).alias("refund_reason"),
                      regexp_extract(col("refund_reason"), "^([^:]+):(.+$)",2).alias("refund_source"))
                      

display(df_regexp_extract_function )


                 # split_column.getItem(0).alias("refund_reason")\
                    #,split_column.getItem(1).alias("refund_source"))


# COMMAND ----------

df_regexp_extract_function.writeTo('gizmobox.silver.py_refunds').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_refunds

# COMMAND ----------

# %sql
# --lets try the same split column based on delimiter or pattern with regexp_extract function
# --lets also split the datetime into 2 columns date and time using date_format() function

# SELECT refund_id,
# payment_id,
# Cast(date_format(refund_timestamp,"yyyy-MM-dd") AS DATE) AS refund_date,
# date_format(refund_timestamp, "HH:mm:ss") AS refund_time,
# refund_amount,
# --refund_reason,
# regexp_extract(refund_reason,'(.+):(.+)',1) as refund_reason,
# --regexp_extract(refund_reason,'(.+):(.+)',2) as refund_source,
# --regexp_extract(refund_reason,'^([^:]+):',1) as refund_reason,
# regexp_extract(refund_reason,'^([^:]+):(.+)$',2) as refund_source
# FROM hive_metastore.bronze.refunds

# COMMAND ----------



# COMMAND ----------

# %sql
# --writing cleansed table into hivemetastore silver schema
# --here we aren't specifying MANAGED LOCATION on the schema.
# --so the data will be stored in dbfs users/hive/warehouse root storage of the managed table
# --avoid storing the data in root storages as these storage containers are attached to databricks workspaces.when you delete workspace , storage gets deleted too.
# --therefore mostly create external managed location and store data by schema or containers as we did previously.

# --creating the schema 1st
# CREATE SCHEMA IF NOT EXISTS hive_metastore.silver;

# CREATE TABLE IF NOT EXISTS hive_metastore.silver.refunds AS 

# --lets try the same split column based on delimiter or pattern with regexp_extract function
# --lets also split the datetime into 2 columns date and time using date_format() function

#   SELECT refund_id,
#   payment_id,
#   Cast(date_format(refund_timestamp,"yyyy-MM-dd") AS DATE) AS refund_date,
#   date_format(refund_timestamp, "HH:mm:ss") AS refund_time,
#   refund_amount,
#   --refund_reason,
#   regexp_extract(refund_reason,'(.+):(.+)',1) as refund_reason,
#   --regexp_extract(refund_reason,'(.+):(.+)',2) as refund_source,
#   --regexp_extract(refund_reason,'^([^:]+):',1) as refund_reason,
#   regexp_extract(refund_reason,'^([^:]+):(.+)$',2) as refund_source
#   FROM hive_metastore.bronze.refunds


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.silver.refunds

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED  hive_metastore.silver.refunds

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED  hive_metastore.silver.refunds
