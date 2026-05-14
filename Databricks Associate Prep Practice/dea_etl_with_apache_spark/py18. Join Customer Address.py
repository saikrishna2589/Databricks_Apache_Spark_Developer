# Databricks notebook source
# MAGIC %md
# MAGIC #### Join Customer and Address
# MAGIC Join customer data with address data to create a customer_address table,which contains the address of each customer on the same record.

# COMMAND ----------


df_customer_details = spark.read.table("gizmobox.silver.customers")

# COMMAND ----------


df_addresses = spark.read.table("gizmobox.silver.addresses")

# COMMAND ----------

df_customer_addresses_join =df_customer_details.alias("c").join(df_addresses.alias("a"),on=df_customer_details.customer_id ==df_addresses.customer_id,
                        how ='inner')

# COMMAND ----------

# %sql
# SELECT * FROM gizmobox.silver.customers

# COMMAND ----------

# # %sql
# SELECT * FROM gizmobox.silver.addresses

# COMMAND ----------

display(df_customer_addresses_join)

# COMMAND ----------

 
df_customer_address_gold = df_customer_addresses_join.select(
       "c.customer_id",
        "email",
        "date_of_birth",
        "member_since",
        "telephone",
        "shipping_address_line",
        "shipping_city",
        "shipping_state",
        "shipping_postcode",
        "billing_address_line",
        "billing_city",
        "billing_postcode")
 


# COMMAND ----------

df_customer_address_gold.writeTo('gizmobox.gold.py_customer_address').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.gold.py_customer_address

# COMMAND ----------

# MAGIC %sql
# MAGIC --joining customers with addresses table and saving into gold table
# MAGIC --so if marketing teams needs customer contact details with their addresses, they can see this table.
# MAGIC --saving the table into gold layer
# MAGIC
# MAGIC -- CREATE TABLE gizmobox.gold.customer_address
# MAGIC -- AS
# MAGIC --     SELECT c.customer_id,
# MAGIC --         c.email,
# MAGIC --         c.date_of_birth,
# MAGIC --         c.member_since,
# MAGIC --         c.telephone,
# MAGIC --         a.shipping_address_line,
# MAGIC --         a.shipping_city,
# MAGIC --         a.shipping_state,
# MAGIC --         a.shipping_postcode,
# MAGIC --         a.billing_address_line,
# MAGIC --         a.billing_city,
# MAGIC --         a.billing_postcode
# MAGIC --     FROM gizmobox.silver.customers AS c
# MAGIC --     INNER JOIN
# MAGIC --     gizmobox.silver.addresses AS a
# MAGIC --     ON c.customer_id = a.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM gizmobox.gold.customer_address
