# Databricks notebook source
# MAGIC %md
# MAGIC #### Monthly Order Summary
# MAGIC For each of the customer, produce the folllwing summary per month.
# MAGIC
# MAGIC 1.  total orders
# MAGIC 2. total items bought
# MAGIC 3. total amount spent

# COMMAND ----------


df_orders = spark.read.table('gizmobox.silver.py_orders_main')

# COMMAND ----------

display(df_orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gizmobox.silver.orders_main
# MAGIC ORDER BY customer_id, order_id 

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import date_trunc, countDistinct,sum

df_order_aggregate = df_orders.groupBy("customer_id",
                 date_trunc("month",df_orders.transaction_timestamp ).alias('month_group'))\
                      .agg(
                          countDistinct(df_orders.order_id).alias('order_count'),
                           sum(df_orders.quantity).alias('order_quantity'),
                           sum(df_orders.price * df_orders.quantity).alias('total_amount_spent')
                      )

# COMMAND ----------

display(df_order_aggregate)

# COMMAND ----------

display(df_order_aggregate)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Montly orders by customer 
# MAGIC
# MAGIC SELECT CAST(DATE_TRUNC("MONTH",transaction_timestamp) AS DATE) AS month_group ,
# MAGIC customer_id,COUNT(DISTINCT order_id) AS order_count,
# MAGIC SUM(quantity) AS total_quantity,
# MAGIC SUM(price*quantity) AS total_amount_spent
# MAGIC FROM gizmobox.silver.orders_main
# MAGIC GROUP BY CAST(DATE_TRUNC("MONTH",transaction_timestamp) AS DATE), customer_id
# MAGIC ORDER BY month_group, order_count DESC,customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --total orders validation check on one customer
# MAGIC
# MAGIC SELECT *
# MAGIC FROM gizmobox.silver.orders_main
# MAGIC WHERE customer_id =5816
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gizmobox.gold.order_summary_monthly
# MAGIC AS
# MAGIC
# MAGIC   SELECT CAST(DATE_TRUNC("MONTH",transaction_timestamp) AS DATE) AS month_group ,
# MAGIC   customer_id,COUNT(DISTINCT order_id) AS order_count,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   SUM(price*quantity) AS total_amount_spent
# MAGIC   FROM gizmobox.silver.orders_main
# MAGIC   GROUP BY CAST(DATE_TRUNC("MONTH",transaction_timestamp) AS DATE), customer_id
# MAGIC   ORDER BY month_group, order_count DESC,customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.gold.order_summary_monthly
# MAGIC

# COMMAND ----------


