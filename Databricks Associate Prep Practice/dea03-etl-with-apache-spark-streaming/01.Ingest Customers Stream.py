# Databricks notebook source
# MAGIC %md
# MAGIC #### Stream Customer Data From Cloud Files to Delta Lake
# MAGIC 1. Read files from cloud storage using Datastreamreader API
# MAGIC 2. Transform the dataframe to add the columns:
# MAGIC -  file path : cloud file path
# MAGIC - ingestion date : Current Timestamp
# MAGIC 3.  Write the transformed data stream to deltalake table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read files using DataStreamReader API
# MAGIC

# COMMAND ----------

#for spark streaming ,schema infering is disabled by default.
#so you need to provide the schema of the data in the source 

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType,TimestampType


schema = StructType(fields = [
                                StructField("customer_id", IntegerType()),
                                StructField("customer_name", StringType()),
                                StructField("date_of_birth", DateType()),
                                StructField("telephone", StringType()),
                                 StructField("email", StringType()),
                                  StructField("member_since", DateType()),
                                   StructField("created_timestamp", TimestampType())]

)

# COMMAND ----------

customers_df = (
    spark.readStream
    .format('json')
    .schema(schema)
    .load('/Volumes/gizmobox/landing/operational_data/customers_stream/')
)

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
                        .option("checkpointLocation","/Volumes/gizmobox/landing/operational_data/customers_stream/_checkpoint_stream")\
                            .toTable('gizmobox.bronze.customers_stream')
)

# COMMAND ----------

streaming_query.stop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.customers_stream

# COMMAND ----------


