# Databricks notebook source
# MAGIC %md
# MAGIC #### Stream Customer Data From Cloud Files to Delta Lake using Auto Loader
# MAGIC 1. Read files from cloud storage using Auto Loader.
# MAGIC 2. Ingest only year 2024 file name data(readstream.option("pathGlobFilter""{*.jpeg,png,jpg)")
# MAGIC 3. more file options-modifiedafter or before.  (Ex- so we use file options in .readstream.option('modifiedAfter':'2025-05-13 12:00:00 UTC+10')
# MAGIC 2. Transform the dataframe to add the columns:
# MAGIC   -  file path : cloud file path
# MAGIC   - ingestion date : Current Timestamp
# MAGIC 3.  Write the transformed data stream to deltalake table
# MAGIC

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

#reading only 2024 files in the name of the file in storage
#for autoloader format is cloudFiles
customers_df =(
spark.readStream
.format('cloudFiles')
.option('cloudFiles.format','json') #format
.option('pathGlobFilter', '*_2024*.json')  
.option('cloudFiles.schemaLocation', "/Volumes/gizmobox/landing/operational_data/customers_autoloader/_schema") #schema location
.option('cloudFiles.inferColumnTypes','True')
.option('cloudFiles.schemaHints', "created_timestamp TIMESTAMP, date_of_birth DATE, member_since DATE")
.load('/Volumes/gizmobox/landing/operational_data/customers_autoloader/')
)

# COMMAND ----------

# # for autoloader format is cloudFiles
# # 
# customers_df = (
# spark.readStream
# .format("cloudFiles")
# .option("cloudFiles.format",'json')
# .option("cloudFiles.schemaLocation", "/Volumes/gizmobox/landing/operational_data/customers_autoloader/_schema")
# .option("cloudFiles.inferColumnTypes" ,"True")
# .option("cloudFiles.schemaHints", "created_timestamp TIMESTAMP, date_of_birth DATE, member_since DATE")
# .load('/Volumes/gizmobox/landing/operational_data/customers_autoloader/'))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Transform the dataframe to add new 2 new columns:
# MAGIC -  Cloud Filepath (_metadata_file_path)
# MAGIC - Ingestion date : current timestamp

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

customer_df_transformed = customers_df.withColumn("file_path", col("_metadata.file_path"))\
                                        .withColumn("ingestion_date", current_timestamp())
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write the transformed data to delta table

# COMMAND ----------

streaming_query =(
                        customer_df_transformed.writeStream.format('delta')\
                        .option("checkpointLocation","/Volumes/gizmobox/landing/operational_data/customers_autoloader/_checkpoint_stream")\
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


