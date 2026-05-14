-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create or Replace & CTAS
-- MAGIC 1. Difference between Create or Replace and Drop and Create Table Statements
-- MAGIC 2. CTAS Statements  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.  Difference between Create or Replace and Drop and Create Table Statements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Behaviour of the DROP and CREATE statements
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE TABLE demo.delta_lake.companies
  ( company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),--AUTOINCREMENT COLUMN
    company_name STRING NOT NULL,--column constraint
  founded_date DATE COMMENT 'this column has founded dates of companies', --column comment
  country STRING,
  founder_year INT GENERATED ALWAYS AS (YEAR(founded_date))) --CALCULATED DERIVED COLUMN
COMMENT
  'This table stores data on some of the most successful tech companies in the world' --table comment
TBLPROPERTIES
  ('sensitive' ='true', 'delta.enableDeletionVectors' ='false') ; --table properties;


INSERT INTO demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('Apple','1976-04-01','USA'),
('Microsoft','1975-04-04','USA'),
('Google','1998-09-04','USA')



-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies;

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Behaviour of CREATE OR REPLACE TABLE STATEMENTS 

-- COMMAND ----------

CREATE OR REPLACE TABLE demo.delta_lake.companies

  ( company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),--AUTOINCREMENT COLUMN
    company_name STRING NOT NULL,--column constraint
  founded_date DATE COMMENT 'this column has founded dates of companies', --column comment
  country STRING,
  founder_year INT GENERATED ALWAYS AS (YEAR(founded_date))) --CALCULATED DERIVED COLUMN
COMMENT
  'This table stores data on some of the most successful tech companies in the world' --table comment
TBLPROPERTIES
  ('sensitive' ='true', 'delta.enableDeletionVectors' ='false') ; --table properties;


INSERT INTO demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('Apple','1976-04-01','USA'),
('Microsoft','1975-04-04','USA'),
('Google','1998-09-04','USA')



-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. CTAS statement and overcoming limitations

-- COMMAND ----------



INSERT INTO demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('IndiaFocus','2027-01-01','India')

-- COMMAND ----------

--CTAS is table based on query result

CREATE OR REPLACE TABLE demo.delta_lake.top_indian_company
AS
SELECT * FROM demo.delta_lake.companies
WHERE country ='India'


-- COMMAND ----------

SELECT * FROM demo.delta_lake.top_indian_company

-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.top_indian_company

-- COMMAND ----------

--CTAS has some limitations over CREATE OR REPLACE TABLE especialyl in column properties
--column data types or column propertiies such as comments on column cant be directly established
--however workaround exists as below . USing CAST for changing datatype and using ALTER TABLE table_name ALTER COLUMN column_name COMMENT ''

-- COMMAND ----------

DROP TABLE demo.delta_lake.top_indian_company

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.delta_lake.companies

-- COMMAND ----------

--changing column data type using CAST and adding NOT NULL constraints and Column COMMENT using ALTER statements
CREATE OR REPLACE TABLE demo.delta_lake.top_indian_company
AS
SELECT 
  CAST(company_id AS INT),
  company_name,
  founded_date,
  country,
  founder_year
 FROM demo.delta_lake.companies
WHERE country ='India'


-- COMMAND ----------

 DESCRIBE TABLE EXTENDED demo.delta_lake.companies

-- COMMAND ----------

--ADD COMMENTS AND NON NULL COLUMN PROPERTIES

ALTER TABLE demo.delta_lake.top_indian_company
 ALTER COLUMN  country SET NOT NULL

-- COMMAND ----------

--ADD COMMENTS AND NON NULL COLUMN PROPERTIES

ALTER TABLE demo.delta_lake.top_indian_company
 ALTER COLUMN  country COMMENT 'countries where the top companies exist'

-- COMMAND ----------

DESCRIBE TABLE EXTENDED  demo.delta_lake.top_indian_company

-- COMMAND ----------


