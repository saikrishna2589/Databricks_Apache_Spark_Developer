# Databricks notebook source
# MAGIC %md
# MAGIC **Note - continuing from '03.Process Addresses Data' notebook**
# MAGIC
# MAGIC #### 3. Apply changes to the Addresses Data (SCD Type 2) -silver_addresses

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773705248022.png](./image_1773705248022.png "image_1773705248022.png")

# COMMAND ----------

# importing modules needed
import dlt
import pyspark.sql.functions as f


# COMMAND ----------

#create streaming table byt iself. no query attached unlike @dlt.table and function that treturns df for write. here we just create table by itself. apply_changes function will populate table .this API will manage the complex merges, _start_at,_end_at SCD type 2 columns that are required, updates ,inserts etc rather than us writing these with complex MERGE UPSERT WHEN match update, not match insert etc and adding the columns start_at,end_at etc for historical tracking.Apply_changes does all these in dlt itself


# in @dlt.table method we used expecations like @dlt.expect, dlt.expect_or_drop etc below @dlt.table and above the function that populates this table. but here in create_streaming_table, you can define expectations within the dlt.create_streaming_table funciton if needed.

dlt.create_streaming_table(
    name ='silver_addresses',
    comment ='SCD Type 2 addresses data',
    table_properties = {'quality' : 'silver'}
)

#

# COMMAND ----------

dlt.apply_changes(
target = 'silver_addresses',
source = 'silver_addresses_clean',
keys = ['customer_id'],  #keys will be in list format
sequence_by = 'created_date',
stored_as_scd_type = 2
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM circuitbox.lakehouse.bronze_addresses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM circuitbox.lakehouse.silver_addresses
