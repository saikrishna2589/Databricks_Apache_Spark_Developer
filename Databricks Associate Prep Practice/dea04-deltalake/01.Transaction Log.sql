-- Databricks notebook source
--create external location for demo container within storage location
--you need external location + storage credential that was used on

-- COMMAND ----------

--Register the path in Unity Catalog metastore for helping to access and authorise the path
CREATE EXTERNAL LOCATION IF NOT EXISTS demo_external_location
URL 'abfss://demo@dbccourseextdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL
dbcourse_ext_sc)
COMMENT 'External location for demo container'

-- COMMAND ----------

--step2 create catalog and provide managed location url so all data within this catalog goes itno the managed location path

CREATE CATALOG IF NOT EXISTS demo
MANAGED LOCATION 'abfss://demo@dbccourseextdl.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Understanding Delta Lake Transaction Log
-- MAGIC
-- MAGIC Understand the cloud storage directory structure behind delta lake tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 0. Create a new schema under delta catalog first

-- COMMAND ----------

--create schema in demo catalog under custom sub folder rather than UC generated folder

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
MANAGED LOCATION  'abfss://demo@dbccourseextdl.dfs.core.windows.net/delta_lake'

-- COMMAND ----------

--DROP SCHEMA IF EXISTS demo.delta_lake

-- COMMAND ----------

--Show schemas in the demo catalog
SHOW SCHEMAS in demo

-- COMMAND ----------

SHOW TABLES in demo.delta_lake

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED  demo.delta_lake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Create a Delta Lake Table
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
(
company_name STRING,
founded_date DATE,
country STRING

);

-- COMMAND ----------

DESCRIBE TABLE EXTENDED  demo.delta_lake.companies

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES
("Apple", "1976-04-01" ,"USA")

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES
("Microsoft", "1975-04-01" ,"USA"),
("Google","1998-09-04","USA"),
("Amazon","1994-07-05","USA")

-- COMMAND ----------


