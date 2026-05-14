# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Transform Payments Data
# MAGIC - Extract DATE AND TIME FROM DATETIME field and create new columns payment_date and payment_time
# MAGIC - Map payment status to contain descriptive values
# MAGIC   (1-Success, 2-Pending, 3-Cancelled, 4-Failed)
# MAGIC - Write transformed data to the silver schema
# MAGIC
# MAGIC

# COMMAND ----------

# %sql
# SELECT payment_id,
# order_id,
# payment_timestamp,
# payment_status,
# payment_method
# FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's extract date and time from datetime field

# COMMAND ----------

df = spark.read.table("gizmobox.bronze.payments")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col,date_format

df_date_time_split = df.select(col("payment_id"), col("order_id"),\
                               date_format(col("payment_timestamp"),"yyyy-MM-dd").cast("date").alias("payment_date"),
                               date_format(col("payment_timestamp"),"HH:mm:SS a").alias("payment_time"),
                               col("payment_status"),col("payment_method") )

        
                                                                       

# COMMAND ----------

display(df_date_time_split)

# COMMAND ----------

# DBTITLE 1,Untitled
# %sql
# --select columns needed

# With bronze_payments AS 
# (
#     SELECT payment_id,
#     order_id,
#     payment_timestamp,
#     payment_status,
#     payment_method
#     FROM gizmobox.bronze.payments
# )

# SELECT payment_id, 
# order_id,
# CAST( date_format(payment_timestamp, "yyyy-MM-dd")  AS DATE) AS payment_date,
# date_format(payment_timestamp, "HH:mm:ss a") AS payment_time,
# payment_status, payment_method
# FROM bronze_payments

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Map payment_status column values to descriptive names instead

# COMMAND ----------

from pyspark.sql.functions import col, when

df_transformed_when_otherwise_date_format_functions =\
    df_date_time_split\
    .withColumn("payment_status",\
                when(col("payment_status") ==1, "Success")\
                    .when(col("payment_status") ==2, "Pending")\
                        .when(col("payment_status") ==3, "Cancelled")\
                            .when(col("payment_status") ==1, "Failed")\
                                .otherwise("other"))

                


# COMMAND ----------

display(df_transformed_when_otherwise_date_format_functions)

# COMMAND ----------

# f%sql
# --select columns needed

# With bronze_payments AS 
# (
#     SELECT payment_id,
#     order_id,
#     payment_timestamp,
#     payment_status,
#     payment_method
#     FROM gizmobox.bronze.payments
# )

# SELECT payment_id, 
# order_id,
# CAST( date_format(payment_timestamp, "yyyy-MM-dd")  AS DATE) AS payment_date,
# date_format(payment_timestamp, "HH:mm:ss a") AS payment_time,
# CASE payment_status
#   WHEN 1 THEN "Success"
#   WHEN 2 THEN "Pending"
#   WHEN 3 THEN "Cancelled"
#   WHEN 4 THEN "Failed"
#   END AS payment_status

# FROM bronze_payments

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Silver table now as you haeve cleasned

# COMMAND ----------

df_transformed_when_otherwise_date_format_functions.writeTo('gizmobox.silver.payments').createOrReplace()

# COMMAND ----------

df_output_table = spark.read.table("gizmobox.silver.payments")
display(df_output_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gizmobox.silver.payments
# MAGIC AS
# MAGIC     With bronze_payments AS 
# MAGIC     (
# MAGIC         SELECT payment_id,
# MAGIC         order_id,
# MAGIC         payment_timestamp,
# MAGIC         payment_status,
# MAGIC         payment_method
# MAGIC         FROM gizmobox.bronze.payments
# MAGIC     )
# MAGIC
# MAGIC     SELECT payment_id, 
# MAGIC     order_id,
# MAGIC     CAST( date_format(payment_timestamp, "yyyy-MM-dd")  AS DATE) AS payment_date,
# MAGIC     date_format(payment_timestamp, "HH:mm:ss a") AS payment_time,
# MAGIC     CASE payment_status
# MAGIC       WHEN 1 THEN "Success"
# MAGIC       WHEN 2 THEN "Pending"
# MAGIC       WHEN 3 THEN "Cancelled"
# MAGIC       WHEN 4 THEN "Failed"
# MAGIC       END AS payment_status
# MAGIC
# MAGIC     FROM bronze_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.payments

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gizmobox.silver.payments
