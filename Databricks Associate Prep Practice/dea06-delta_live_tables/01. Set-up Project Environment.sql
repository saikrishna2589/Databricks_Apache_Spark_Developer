-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Set-up the project environment for CircuitBox Data Lakehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create external location -dbccourseextdl_circuitbox
-- MAGIC 2. Create Catalog - circuitbox 
-- MAGIC 3. Create Schemas
-- MAGIC     - landing
-- MAGIC     - lakehouse
-- MAGIC 4. Create Volume -operational_data
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image_1773114691830.png](./image_1773114691830.png "image_1773114691830.png")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1 . Create External Location
-- MAGIC
-- MAGIC - **External Location Name** : dbccourseextdl_circuitbox
-- MAGIC - ADLS path : 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/'
-- MAGIC - Storage Credential : dbcourse_ext_sc

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dbccourseextdl_circuitbox
URL 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL dbcourse_ext_sc)
COMMENT 'External Location for circuitbox data lakehouse'

-- COMMAND ----------

-- testing if we can access the external location url 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/'

LIST 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Catalog name :circuitbox
-- MAGIC - external path : 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/'

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS circuitbox
MANAGED LOCATION 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/'
COMMENT 'Catalog for the circuitbox data lakehouse'


-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create Schemas
-- MAGIC
-- MAGIC
-- MAGIC 1. Schema Name:landing
-- MAGIC -     Managed Location : 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/landing'
-- MAGIC
-- MAGIC 2. Schema Name:lakehouse
-- MAGIC -   Managed Location : 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/lakehouse'
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

USE CATALOG circuitbox

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing
MANAGED LOCATION  'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/landing'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS lakehouse
MANAGED LOCATION  'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/lakehouse'

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Create Volume
-- MAGIC
-- MAGIC - Volume_name : Operational_data
-- MAGIC - ADLS path :  'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/landing/operational_data'

-- COMMAND ----------

USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
 LOCATION 'abfss://circuitbox@dbccourseextdl.dfs.core.windows.net/landing/operational_data'

-- COMMAND ----------

--Access files from this volume to test if it works

LIST '/Volumes/circuitbox/landing/operational_data'

-- COMMAND ----------

SHOW CATALOGS;

USE CATALOG circuitbox

-- COMMAND ----------

SHOW SCHEMAS;
USE SCHEMA lakehouse

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM bronze_customers

-- COMMAND ----------

SELECT * FROM silver_customers_clean

-- COMMAND ----------

SELECT * FROM silver_customers

-- COMMAND ----------


