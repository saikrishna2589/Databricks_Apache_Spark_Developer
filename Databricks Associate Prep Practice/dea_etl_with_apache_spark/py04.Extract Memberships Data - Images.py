# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data from memberships images
# MAGIC 1. Query orders file using binary format for images
# MAGIC 2. Save the data in a view

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Query orders file using binary format for images

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/memberships'

# COMMAND ----------

df=spark.read.format("binaryFile").load("/Volumes/gizmobox/landing/operational_data/memberships/*/*.png")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# %sql
# SELECT * 
# FROM 
# binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Save the data

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_memberships').createOrReplace()

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
# AS
# SELECT * 
# FROM 
# binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM  
# MAGIC gizmobox.bronze.py_memberships

# COMMAND ----------


