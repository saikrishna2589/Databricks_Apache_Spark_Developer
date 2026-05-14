# Databricks notebook source
# MAGIC %md
# MAGIC #### Stream Customer Data From Cloud Files to Delta Lake using Auto Loader
# MAGIC 1. Read files from cloud storage using Auto Loader
# MAGIC 2.  Schema Evolution options parameters
# MAGIC 2. Transform the dataframe to add the columns:
# MAGIC   -  file path : cloud file path
# MAGIC   - ingestion date : Current Timestamp
# MAGIC 3.  Write the transformed data stream to deltalake table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Auto Loader Schema Evolution (cloudFiles.schemaEvolutionMode)
# MAGIC Controls how Auto Loader handles new columns in incoming files:
# MAGIC
# MAGIC - **addNewColumns** (your setting): Automatically adds new columns to the schema when detected
# MAGIC Example: Initial file has customer_id, name, email → New file arrives with customer_id, name, email, phone → Auto Loader automatically adds phone column
# MAGIC rescue: Puts unexpected columns into a special _rescued_data column as JSON
# MAGIC Example: New column phone → Stored as {"phone": "555-1234"} in _rescued_data
# MAGIC
# MAGIC - **failOnNewColumns**: Stops the stream when new columns appear (default behavior)
# MAGIC Example: New column phone detected → Stream fails with schema mismatch error
# MAGIC
# MAGIC - **none**: Ignores new columns completely
# MAGIC Example: New column phone → Silently dropped, not written to table
# MAGIC
# MAGIC **Delta Merge Schema (mergeSchema)**
# MAGIC Controls how Delta Lake handles schema changes when writing:
# MAGIC
# MAGIC "true" (your setting): Merges new columns from DataFrame into existing Delta table
# MAGIC Example: Table has 5 columns → Stream brings 6 columns → Delta adds the 6th column to table schema
# MAGIC
# MAGIC "false" (default): Rejects writes if schemas don't match exactly
# MAGIC Example: Extra column in DataFrame → Write fails with schema mismatch error
# MAGIC Together: Auto Loader detects new columns in files → addNewColumns adds them to streaming DataFrame → mergeSchema allows Delta to accept and add them to the target table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read files using DataStreamReader API
# MAGIC

# COMMAND ----------

#no need of schema explicitly mentioning for autoloader method unlike readstrem method.
# 
#  #for spark streaming ,schema infering is disabled by default.
# #so you need to provide the schema of the data in the source 

# from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType,TimestampType


# schema = StructType(fields = [
#                                 StructField("customer_id", IntegerType()),
#                                 StructField("customer_name", StringType()),
#                                 StructField("date_of_birth", DateType()),
#                                 StructField("telephone", StringType()),
#                                  StructField("email", StringType()),
#                                   StructField("member_since", DateType()),
#                                    StructField("created_timestamp", TimestampType())]

)

# COMMAND ----------

# for autoloader format is cloudFiles
# 
customers_df = (
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format",'json')
.option("cloudFiles.schemaLocation", "/Volumes/gizmobox/landing/operational_data/customers_autoloader/_schema")
.option("cloudFiles.schemaEvolutionMode" , "addNewColumns")
.option("cloudFiles.inferColumnTypes" ,"True")
.option("cloudFiles.schemaHints", "created_timestamp TIMESTAMP, date_of_birth DATE, member_since DATE")
.load('/Volumes/gizmobox/landing/operational_data/customers_autoloader/')
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Transform the dataframe to add new 2 new columns:
# MAGIC -  Cloud Filepath (_metadata_file_path)
# MAGIC - Ingestion date : current timestamp

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

customers_df_transformed = customers_df.withColumn("file_path", col("_metadata.file_path"))\
                                        .withColumn("ingestion_date", current_timestamp())
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write the transformed data to delta table

# COMMAND ----------

streaming_query =(
                        customers_df_transformed.writeStream.format('delta')\
                        .option("checkpointLocation","/Volumes/gizmobox/landing/operational_data/customers_autoloader/_checkpoint_stream")\
                            .option("mergeSchema", "true")
                            .toTable('gizmobox.bronze.customers_autoloader')
)

# COMMAND ----------

streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE gizmobox.bronze.customers_autoloader
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.customers_autoloader
# MAGIC

# COMMAND ----------


