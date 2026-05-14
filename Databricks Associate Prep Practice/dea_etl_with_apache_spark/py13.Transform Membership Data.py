# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Memberships Data
# MAGIC 1. Extract customer_id from the file path
# MAGIC 2. Write trasformed data to silver schema

# COMMAND ----------

# DBTITLE 1,Untitled

df = spark.read.table("gizmobox.bronze.v_memberships")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import col, regexp_extract
df_regexp_extract_memberid = df.select\
    (regexp_extract(col("path"),'(.+)/(\\d+)\\.(png)$',2).alias('customer_id'), col("content").alias("image"))

display(df_regexp_extract_memberid)

# COMMAND ----------

df_regexp_extract_memberid.writeTo('gizmobox.silver.py_memberships').createOrReplace()

# COMMAND ----------

# DBTITLE 1,Cell 6

df_read = spark.read.table("gizmobox.silver.py_memberships")
display(df_read)

# COMMAND ----------

# DBTITLE 1,Untitled
# %sql
# --'.' in regex means single character. so if u want to say in pattern literal (.) i.e full stop,
# --you escape the character with '\\.' . then it represents 

# CREATE TABLE gizmobox.silver.memberships
# AS 
#   SELECT
#   regexp_extract(path, '^(.+)/(\\d+)\\.(png)$' ,2) AS customer_id,
#   content as image
#   FROM gizmobox.bronze.v_memberships

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM gizmobox.silver.memberships

# COMMAND ----------


