# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract data in CSV (tab delimited and header) format.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Limiation of Select statement --can't specify the header, tab delimited etc to the statement so the read doen's get parsed properly.(instead beter to use read_files() function or create external table as data is already present and specify these options and then read of that )

# COMMAND ----------

# DBTITLE 1,Untitled

df = spark.read.format('csv') \
            .option('header',True)\
            .option('sep' ,'\t')\
            .load('/Volumes/gizmobox/landing/operational_data/addresses')

display(df)


# COMMAND ----------

# %sql
# -- address data in azure has header and is tab delimited.
# SELECT * 
# FROM csv.`/Volumes/gizmobox/landing/operational_data/addresses`

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/addresses'

# COMMAND ----------

# MAGIC %md
# MAGIC #### we need to tell spark that there is header and tab seperated. lets use read_files()

# COMMAND ----------

# %sql
# SELECT *
# FROM 
# read_files('/Volumes/gizmobox/landing/operational_data/addresses',
# format =>'csv',
# header=>true,
# delimiter=> '\t')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Create Addresses view in Bronze Layer
# MAGIC

# COMMAND ----------

df.writeTo('gizmobbox.bronze.py_addresses')

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW gizmobox.bronze.v_addresses 
# AS
# SELECT *
# FROM 
# read_files('/Volumes/gizmobox/landing/operational_data/addresses',
# format =>'csv',
# header=>true,
# delimiter=> '\t')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC gizmobbox.bronze.py_addresses 

# COMMAND ----------


