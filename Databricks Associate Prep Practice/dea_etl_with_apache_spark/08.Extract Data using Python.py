# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data using Pyspark
# MAGIC
# MAGIC 1. Run SQL Commands using Python -spark.sql functions
# MAGIC 2. Spark dataframe reader API
# MAGIC 3. Read tables using spark.table function

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Run SQL commands using Python - spark.sql Function

# COMMAND ----------

# DBTITLE 1,Untitled
df = spark.sql(
    "SELECT * \
          FROM json.`/Volumes/gizmobox/landing/operational_data/customers`"
          );

display(df)

# COMMAND ----------

#ddl statement example using spark.sql

df1= spark.sql(
        '''CREATE OR REPLACE TEMP VIEW python_spark_sql
         AS \
        SELECT * \
            FROM json.`/Volumes/gizmobox/landing/operational_data/customers`'''
            );

# COMMAND ----------

# MAGIC %sql
# MAGIC --using sql to access the view created using spark.sql pyspark rapper
# MAGIC
# MAGIC SELECT * FROM  python_spark_sql
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Spark DataFrameReader API
# MAGIC
# MAGIC

# COMMAND ----------

read_data_df = spark.read.format("csv").options(sep ='\t',header= False).load('/Volumes/gizmobox/landing/operational_data/customers')

display(read_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 3. Spark.table function

# COMMAND ----------

df2 = spark.table('gizmobox.bronze.payments')
display(df2)

# COMMAND ----------


