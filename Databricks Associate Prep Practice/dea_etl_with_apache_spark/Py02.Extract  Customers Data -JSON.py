# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Customers JSON file
# MAGIC 1. Query Single File
# MAGIC 2. Quey List of Files using wildcard characters
# MAGIC 3. Query all the files in a folder
# MAGIC 4. Select file metadata hidden (using read_files() function )
# MAGIC 5. Create Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Query Single JSON file

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/customers'

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Read data using pyspark reader API

# COMMAND ----------

df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 3. Read data using spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC    --SELECT * 
# MAGIC    --FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Query multiple files

# COMMAND ----------

df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json')

display(df)

# COMMAND ----------

# %sql
#    SELECT * 
#    FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Query all files in the  folder

# COMMAND ----------

df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/customers')

display(df)

# COMMAND ----------

# %sql
#    SELECT * 
#    FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. USING dataframe reader json method shortcut instead of format method above

# COMMAND ----------

df=spark.read.json('/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 7. #### Access metadata such as file_path and add it to the data columns

# COMMAND ----------

# DBTITLE 1,Untitled
#read data
df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/customers')

# transform
df_with_metadata =df.select('*','_metadata.file_name', '_metadata.file_path', '_metadata.file_size')

display(df_with_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# %sql
# -- once volume is created, you don't need to use abfss protocol url link. you can use instead simple path

# SELECT * FROM read_files('/Volumes/gizmobox/landing/operational_data',format=>'tsv',sep=>'\t')

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data'

# COMMAND ----------

# %skip
# %sql
# SELECT * , _metadata.file_path
# FROM 
# read_files('/Volumes/gizmobox/landing/operational_data/customers',format=>'json')

# COMMAND ----------

# %sql
# SELECT COUNT(*) 
# FROM 
# read_files('/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json',format=>'json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC - ###### There are 2 APIs for writing data into df after transforming. v1 and v2 . v2 is recommended .it is sepearate create, replace etc method than v1 ,which is method as below.
# MAGIC - ##### v2 is integrated directly with unity catalog so thats better

# COMMAND ----------

#v1 api
df_with_metadata.write.format("delta").mode('overwrite').saveAsTable('gizmobox.bronze.py_customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_customer

# COMMAND ----------

# v2 API method. This is more preffered.
df_with_metadata.writeTo('gizmobox.bronze.py_customer_v2').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_customer_v2

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Creating a VIEW 

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW gizmobox.bronze.v_customers
# AS
# SELECT * , _metadata.file_path
# FROM 
# read_files('/Volumes/gizmobox/landing/operational_data/customers',format=>'json')


# COMMAND ----------

# DBTITLE 1,Cell 16
# %sql
# DESCRIBE TABLE EXTENDED gizmobox.bronze.v_customers

# COMMAND ----------


