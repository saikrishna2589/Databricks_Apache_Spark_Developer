# Databricks notebook source
# MAGIC %md
# MAGIC #### Proces Address Data -Python way! 
# MAGIC
# MAGIC **Having processed incremental dlt pipeline using autoloader and streaming services using SQL, we shall now look at python.**
# MAGIC
# MAGIC 1. Ingest the data into the data lakehouse -bronze_addresses
# MAGIC 2. Perform the data quality checks and transform the data as required -silver_addresses_clean
# MAGIC 3. Apply changes to the Addresses data (SCD Type 2) - silver_addresses

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### 1. Ingest the data into the data lakehouse- bronze_addressess

# COMMAND ----------

import dlt
import pyspark.sql.functions as f


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773636584167.png](./image_1773636584167.png "image_1773636584167.png")

# COMMAND ----------


#ingest data from volume to bronze layer dlt streaming data.

@dlt.table(name= 'bronze_addresses', 
           comment ='bronze_address_table',
           table_properties = {'quality' :'bronze'}
           )#'@dlt' creates the delta live tables.# In the parameter , we providename of the table and its properties.

#define function that returns dataframe. add hidden metadata column for file path and add timestamp column too.
def bronze_address():
    return (
            spark.readStream
            .format('cloudFiles') #using autoloader for checking new files,helps with auto checkpoints too.
            .option('cloudFiles.format' , "csv")
            .option('cloudFiles.inferColumnTypes' ,'True')
            .load('/Volumes/circuitbox/landing/operational_data/addresses//')
            .select('*',
                    f.col("_metadata.file_path").alias("input_file_path"),
                    f.current_timestamp().alias("ingest_timestamp")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Read data from bronze table , clean the data using  dlt expectations and load into streaming silver cleaned table.

# COMMAND ----------




#dlt table definition to create dlt table using returned df in function.
@dlt.table(name ='silver_addresses_clean', 
           comment='cleaned_address_data', 
           table_properties ={'quality' : 'silver'}
           )


#apply expectations on the columns for data quality
@dlt.expect('valid_postcode', 'LENGTH(postcode)=5') #warning
@dlt.expect_or_drop('valid_address_line', 'address_line_1 IS  NOT NULL')  #drop
@dlt.expect_or_fail ('valid_customer_id', 'customer_id IS NOT NULL')  #fail update pipeline

#create function that returns df with selected columns
def clean_silver_table():
  return (

    spark.readStream.table('LIVE.bronze_addresses')  #reading the bronze_details streaming table.spark.readStream.table. LIVE keyword is a virtual schema that tells DLT that this all will be the same pipeline. 
    .select ("customer_id",
             "address_line_1",
             "city",
             "state",
             "postcode",
             f.col('created_date').cast('DATE'))
    )
  

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773704772728.png](./image_1773704772728.png "image_1773704772728.png")

# COMMAND ----------


