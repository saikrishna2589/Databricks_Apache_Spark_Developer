# Databricks notebook source
# %md
# #### Extract Data From the Returns SQL Table
# 1. Create Bronze Schema in the Hive Metastore
# 2. Create External Table

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read Returns Data Via JDBC
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.read\
    .format('jdbc')\
    .options(url ='jdbc:sqlserver://gizmobox-sql-server-databricks-examprep.database.windows.net:1433;database=gizmobox-sql-server;encrypt=true;trustServerCertificate=true',
             dbtable ='refunds',
             username='gizmoboxadmin',
             password ='Furylol1@@2589').load()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Write df into table using spark write API
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 7
df.writeTo('gizmobox.bronze.py_refunds').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_refunds

# COMMAND ----------

# DBTITLE 1,Cell 5
#  CREATE TABLE IF NOT EXISTS hive_metastore.bronze.refunds
#  USING JDBC
# OPTIONS
# (
#   url 'jdbc:sqlserver://gizmobox-sql-server-databricks-examprep.database.windows.net:1433;database=gizmobox-sql-server;encrypt=true;trustServerCertificate=true',
#   dbtable 'refunds',
#  user 'gizmoboxadmin',
#   password 'Furylol1@@2589'
#  )

# COMMAND ----------

# %sql
# SELECT * FROM hive_metastore.bronze.refunds

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Tables: Data Movement Patterns
# MAGIC
# MAGIC ### Key Concept: Where Does Data Live?
# MAGIC
# MAGIC **External Table** = Metadata pointer in Databricks + Data stored elsewhere
# MAGIC
# MAGIC Let's explore different types and their data movement patterns...

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1️⃣ JDBC External Tables - NO DATA MOVEMENT
# MAGIC
# MAGIC **Data Location:** Stays in source database (SQL Server, PostgreSQL, MySQL, etc.)
# MAGIC
# MAGIC **How it works:**
# MAGIC - Databricks stores only metadata (table schema, connection info)
# MAGIC - Every query is sent to the source database
# MAGIC - Database executes the query and returns results
# MAGIC - Data travels over network for each query
# MAGIC
# MAGIC **When to use:**
# MAGIC - Small reference tables (< 1GB)
# MAGIC - Real-time data access required
# MAGIC - Source database has good query performance
# MAGIC - Infrequent queries
# MAGIC
# MAGIC **When NOT to use:**
# MAGIC - Large datasets (> 10GB)
# MAGIC - Complex transformations needed
# MAGIC - Frequent queries (network overhead)
# MAGIC - Need for historical snapshots

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2️⃣ File-Based External Tables - NO DATA MOVEMENT (Data Already in Files)
# MAGIC
# MAGIC **Data Location:** Files in cloud storage (ADLS, S3, DBFS)
# MAGIC
# MAGIC **How it works:**
# MAGIC - Databricks stores metadata (schema, file location)
# MAGIC - Data files already exist in storage
# MAGIC - Spark reads files directly when queried
# MAGIC - No data copying - just reading from storage
# MAGIC
# MAGIC **When to use:**
# MAGIC - Data already exists as files in cloud storage
# MAGIC - Large datasets (TBs of data)
# MAGIC - Need to query existing data lakes
# MAGIC - Multiple systems need to access same files
# MAGIC - Cost-effective storage (separate compute from storage)
# MAGIC
# MAGIC **Formats:** Delta, Parquet, CSV, JSON, Avro, ORC

# COMMAND ----------

# DBTITLE 1,Example 2a: Delta External Table
# %sql
# -- Scenario: You have Delta files in Azure Data Lake Storage
# -- Data stays in ADLS, Databricks just reads it

# CREATE TABLE IF NOT EXISTS hive_metastore.bronze.customers_external
# USING DELTA
# LOCATION 'abfss://bronze@yourstorage.dfs.core.windows.net/customers/';

# -- Data is NOT copied to Databricks
# -- Spark reads Delta files directly from ADLS when you query
# SELECT * FROM hive_metastore.bronze.customers_external LIMIT 10;

# -- ✅ Best for: Large datasets, data lake architecture
# -- ✅ Performance: Excellent (columnar format, predicate pushdown)
# -- ✅ ACID transactions supported

# COMMAND ----------

# DBTITLE 1,Example 2b: Parquet External Table
# %sql
# -- Scenario: You have Parquet files from another system
# -- Data stays in storage, no movement

# CREATE TABLE IF NOT EXISTS hive_metastore.bronze.orders_external
# USING PARQUET
# LOCATION 'abfss://bronze@yourstorage.dfs.core.windows.net/orders/';

# -- Reads Parquet files directly from storage
# SELECT order_id, order_date, total_amount 
# FROM hive_metastore.bronze.orders_external 
# WHERE order_date >= '2025-01-01';

# -- ✅ Best for: Existing Parquet data lakes
# -- ✅ Performance: Very good (columnar, compressed)
# -- ❌ No ACID transactions (read-only for external)

# COMMAND ----------

# DBTITLE 1,Example 2c: CSV External Table
# %sql
# -- Scenario: You have CSV files uploaded to storage
# -- Data stays as CSV files, read on-demand

# CREATE TABLE IF NOT EXISTS hive_metastore.bronze.products_external
# USING CSV
# OPTIONS (
#   header 'true',
#   inferSchema 'true',
#   delimiter ','
# )
# LOCATION 'abfss://bronze@yourstorage.dfs.core.windows.net/products/';

# -- Reads CSV files from storage (slower than Parquet/Delta)
# SELECT * FROM hive_metastore.bronze.products_external;

# -- ⚠️ Performance: Slower (row-based format, no compression)
# -- ✅ Best for: Small datasets, quick prototyping
# -- ❌ Not recommended for production large datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3️⃣ Managed Tables - DATA MOVEMENT HAPPENS
# MAGIC
# MAGIC **Data Location:** Copied into Databricks-managed storage
# MAGIC
# MAGIC **How it works:**
# MAGIC - Data is copied from source to Databricks storage
# MAGIC - Databricks manages both metadata AND data files
# MAGIC - Dropping table deletes both metadata and data
# MAGIC
# MAGIC **When to use:**
# MAGIC - Need full control over data lifecycle
# MAGIC - Want Databricks to manage storage
# MAGIC - Building data warehouse/lakehouse
# MAGIC - Need optimal query performance
# MAGIC
# MAGIC **This is different from external tables!**

# COMMAND ----------

# DBTITLE 1,Example 3: Managed Table (Data Copied)
# %sql
# -- Scenario: Copy data from JDBC source to Databricks-managed Delta table
# -- This MOVES data from SQL Server to Databricks storage

# CREATE TABLE hive_metastore.bronze.refunds_managed AS
# SELECT * FROM hive_metastore.bronze.refunds_jdbc;

# -- ✅ Data is now COPIED to Databricks-managed storage
# -- ✅ Much faster queries (no network calls to SQL Server)
# -- ✅ Can perform transformations, updates, deletes
# -- ✅ Optimized for analytics workloads

# -- Query performance comparison:
# -- External JDBC: Slow (network latency, database load)
# -- Managed Delta: Fast (local storage, optimized format)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Movement Summary
# MAGIC
# MAGIC | Table Type | Data Location | Data Movement | Query Performance | Use Case |
# MAGIC |------------|---------------|---------------|-------------------|----------|
# MAGIC | **JDBC External** | Source database | ❌ None (stays in DB) | 🐌 Slow (network) | Small reference tables, real-time |
# MAGIC | **Delta External** | Cloud storage files | ❌ None (stays in files) | 🚀 Fast (columnar) | Large data lakes, shared data |
# MAGIC | **Parquet External** | Cloud storage files | ❌ None (stays in files) | ⚡ Fast (columnar) | Existing Parquet lakes |
# MAGIC | **CSV External** | Cloud storage files | ❌ None (stays in files) | 🐢 Slower (row-based) | Small files, prototyping |
# MAGIC | **Managed Table** | Databricks storage | ✅ **Copied** | 🚀 Fastest | Data warehouse, transformations |
# MAGIC
# MAGIC ### Key Insight:
# MAGIC - **External tables** = Metadata only, data stays at source
# MAGIC - **Managed tables** = Data copied to Databricks storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Common Pattern: Bronze → Silver → Gold
# MAGIC
# MAGIC ### Bronze Layer (Raw Data Ingestion)
# MAGIC ```
# MAGIC JDBC External Table (SQL Server)
# MAGIC         ↓ 
# MAGIC    COPY DATA (one-time or scheduled)
# MAGIC         ↓
# MAGIC Managed Delta Table (Bronze)
# MAGIC ```
# MAGIC
# MAGIC ### Why Copy from External to Managed?
# MAGIC 1. **Performance**: Avoid network latency on every query
# MAGIC 2. **Transformation**: Can't modify external JDBC tables
# MAGIC 3. **History**: Capture snapshots over time
# MAGIC 4. **Cost**: Reduce load on source database
# MAGIC 5. **Reliability**: Don't depend on external system availability
# MAGIC
# MAGIC ### Typical Workflow:
# MAGIC 1. Create JDBC external table (metadata pointer)
# MAGIC 2. Extract data and write to managed Delta table
# MAGIC 3. Perform transformations on managed tables
# MAGIC 4. Drop or keep external table for incremental loads

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤔 Decision Tree: Which Table Type?
# MAGIC
# MAGIC ### Start Here: Where is your data?
# MAGIC
# MAGIC **📍 Data in relational database (SQL Server, PostgreSQL, MySQL)?**
# MAGIC - Small dataset (< 1GB) + infrequent queries → **JDBC External Table**
# MAGIC - Large dataset OR frequent queries → **Extract to Managed Delta Table**
# MAGIC
# MAGIC **📍 Data already in cloud storage as files?**
# MAGIC - Delta format → **Delta External Table** (best performance)
# MAGIC - Parquet format → **Parquet External Table** (good performance)
# MAGIC - CSV/JSON → **CSV/JSON External Table** (okay for small data)
# MAGIC - Need to transform → **Copy to Managed Delta Table**
# MAGIC
# MAGIC **📍 Building new data warehouse/lakehouse?**
# MAGIC - Always use **Managed Delta Tables**
# MAGIC
# MAGIC **📍 Need to share data with other systems?**
# MAGIC - Use **External Tables** (data accessible outside Databricks)
# MAGIC
# MAGIC **📍 Need ACID transactions, updates, deletes?**
# MAGIC - Use **Managed Delta Tables**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Your Current Situation: Refunds Data
# MAGIC
# MAGIC ### What You Have Now:
# MAGIC ```sql
# MAGIC CREATE TABLE hive_metastore.bronze.refunds USING JDBC ...
# MAGIC ```
# MAGIC - ✅ Data stays in SQL Server
# MAGIC - ❌ Every query hits SQL Server (slow, network overhead)
# MAGIC - ❌ Can't transform or enrich data
# MAGIC - ❌ No historical snapshots
# MAGIC
# MAGIC ### Recommended Next Step:
# MAGIC **Extract once, query many times**
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Keep JDBC external table for reference
# MAGIC -- (already done)
# MAGIC
# MAGIC -- Step 2: Copy data to managed Delta table
# MAGIC CREATE TABLE hive_metastore.bronze.refunds_managed
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM hive_metastore.bronze.refunds;
# MAGIC
# MAGIC -- Step 3: Use managed table for all analytics
# MAGIC SELECT 
# MAGIC   refund_reason,
# MAGIC   COUNT(*) as refund_count,
# MAGIC   SUM(refund_amount) as total_refunded
# MAGIC FROM hive_metastore.bronze.refunds_managed
# MAGIC GROUP BY refund_reason;
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC - 🚀 10-100x faster queries
# MAGIC - 💾 Can add transformations
# MAGIC - 📸 Capture historical snapshots
# MAGIC - 💰 Reduce load on SQL Server

# COMMAND ----------

# MAGIC %md
# MAGIC so sql database -->create external table jdbc connector in hive metastore-->u can then copy data to managed table by creating it in UC.
