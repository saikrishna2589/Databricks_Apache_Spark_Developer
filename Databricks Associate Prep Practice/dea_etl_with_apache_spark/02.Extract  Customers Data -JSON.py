# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Customers JSON file
# MAGIC 1. Query Single File
# MAGIC 2. Quey List of Files using wildcard characters
# MAGIC 3. Query all the files in a folder
# MAGIC 4. Select file metadata hidden (using read_files() function )
# MAGIC 5. Register the VIEW in Untiy Catalog by Saving the query SELECT that retunrs structured data as a VIEW

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Query Single JSON file

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT * 
# MAGIC    FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Query multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT * 
# MAGIC    FROM json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Query all files in the  folder

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT * 
# MAGIC    FROM json.`/Volumes/gizmobox/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- once volume is created, you don't need to use abfss protocol url link. you can use instead simple path
# MAGIC
# MAGIC SELECT * FROM read_files('/Volumes/gizmobox/landing/operational_data',format=>'tsv',sep=>'\t')

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * , _metadata.file_path
# MAGIC FROM 
# MAGIC read_files('/Volumes/gizmobox/landing/operational_data/customers',format=>'json')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM 
# MAGIC read_files('/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json',format=>'json')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Creating a VIEW 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gizmobox.bronze.v_customers
# MAGIC AS
# MAGIC SELECT * , _metadata.file_path
# MAGIC FROM 
# MAGIC read_files('/Volumes/gizmobox/landing/operational_data/customers',format=>'json')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gizmobox.bronze.v_customers

# COMMAND ----------


