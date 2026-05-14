-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #### Process the Customers Data
-- MAGIC 1. Ingest data into the data lakehouse-bronze_customers
-- MAGIC 2. Perform data quality checks and transform the data as required - silver_customers_clean
-- MAGIC 3. Apply changes to the Customers data - silver_customers
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image_1773231478312.png](./image_1773231478312.png "image_1773231478312.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE OR REFRESH STREAMING TABLE bronze_customers
-- MAGIC

-- COMMAND ----------

--Create DLT streaming table that we can run from DLT pipeline workflow. 
--read all files in the volume path (i.e * in select below) append mode-i.e streaming data and discover new files using Autoloader(cloudfiles)
--add 2 additional columns too filepath, current timestamp

CREATE OR REFRESH STREAMING TABLE bronze_customers  --DLT table activated with 'CREATE OR REFRESH'. streaming engine actiavted with STREAMING keyword for incremental load
COMMENT 'Raw customer data ingested from the source system ,which is from the volume assosaited with folder in Azure storage account.'
TBLPROPERTIES('quality' ='bronze')
AS SELECT *, _metadata.file_path as input_file_path, current_timestamp() as ingestion_timestamp
FROM cloud_files('/Volumes/circuitbox/landing/operational_data/customers/', 'json',
                   map('cloudFiles.inferColumnTypes', 'true'))  ;


--DLT Lakeflow declarative pipelines help with schema evolution, schema changes ,checkpoint created and managed byDLT, retries management etc. so DLT is more a orchestration layer .

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Clean the bronze table data based on the rules below and load into Silver table using EXPECATIONS DLT 
-- MAGIC - Basically tell it what to do and will do it. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image_1773285550036.png](./image_1773285550036.png "image_1773285550036.png")

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_customers_clean
--data quality expectations
(
CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)
ON VIOLATION FAIL UPDATE ,
CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL)
ON VIOLATION DROP ROW ,
CONSTRAINT valid_telephone EXPECT (length(telephone)>10),
CONSTRAINT valid_email EXPECT (email is NOT NULL),
CONSTRAINT valid_date_of_birth EXPECT (date_of_birth >='1920-01-01')
)

COMMENT 'cleaned customer data'
TBLPROPERTIES ('quality' ='silver')
AS

SELECT customer_id,
customer_name,
CAST(date_of_birth AS DATE) as date_of_birth,
telephone,
email,
CAST(created_date AS DATE) AS CREATED_DATE
FROM STREAM(LIVE.bronze_customers) --LIVE keyword means table created in the same DLT pipeline.
--STREAM keyword would mean DLT would only read the new data from the data since last execution i.e it will process data incrementally.If  we don't use STREAM, all data will be read during execution SO WE ARE using BRONZE customer table as streaming source.

-- COMMAND ----------

--implementing slowly changing dimenstion SCD Type 1 table.
--No history needs to be maintained for dim changes and overwtires when updated record ex-customer address.
--we can use APPLY CHANGES API. We first need to create table as Apply changes dont create table. 
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'SCD Type 1 customers data'
TBLPROPERTIES ('quality'='silver');

-- COMMAND ----------

APPLY CHANGES INTO LIVE.silver_customers --LIVE would mean same DLT pipeline
FROM STREAM(LIVE.silver_customers_clean) --STREAM woudl make this incremental load and not full load.
KEYS (customer_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1  --optional. Type 1 is the defualt value



-- COMMAND ----------

--SHOW CATALOGS

-- COMMAND ----------

--USE CATALOG circuitbox

-- COMMAND ----------

--SHOW SCHEMAS IN circuitbox

-- COMMAND ----------

--USE SCHEMA lakehouse

-- COMMAND ----------

--SHOW TABLES in circuitbox.lakehouse

-- COMMAND ----------

--SELECT * FROM circuitbox.lakehouse.silver_customers

-- COMMAND ----------


