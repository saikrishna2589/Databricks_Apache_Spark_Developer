-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Table - Table and Column Properties
-- MAGIC Demonstrate adding table and column properties to the CREATE TABLE statement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1.Table Properties
-- MAGIC
-- MAGIC - 1.1 COMMENT - allows you to document the purpose of the table
-- MAGIC - 1.2  TBLPROPERTIES - used to specify table level metadata or configuration settings

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;
CREATE TABLE demo.delta_lake.companies 
( company_name STRING,
founded_date DATE,
country STRING)
COMMENT 'This table stores data on some of the most successful tech companies in the world'
TBLPROPERTIES ('sensitive' ='true', 'delta.enableDeletionVectors' ='false')


-- COMMAND ----------

SELECT * 
FROM demo.delta_lake.companies 

-- COMMAND ----------

DESCRIBE EXTENDED demo.delta_lake.companies 

-- COMMAND ----------

DESCRIBE DETAIL demo.delta_lake.companies

-- COMMAND ----------

DESCRIBE TABLE EXTENDED  demo.delta_lake.companies

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Column properties
-- MAGIC
-- MAGIC - 2.1 NOT NULL contraints - enforces data integrity and quality by ensuting that a column cannot containt NULL VALUES
-- MAGIC 2.2 COMMENT - documents the purpose or context of individual columns in a table.

-- COMMAND ----------

-- DBTITLE 1,Cell 7
DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE TABLE demo.delta_lake.companies
  ( company_name STRING NOT NULL,--column constraint
  founded_date DATE COMMENT 'this column has founded dates of companies', --column comment
  country STRING)
COMMENT
  'This table stores data on some of the most successful tech companies in the world' --table comment
TBLPROPERTIES
  ('sensitive' ='true', 'delta.enableDeletionVectors' ='false') --table properties

-- COMMAND ----------

DESCRIBE  TABLE EXTENDED  demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For primary key -Autoincrement :GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT WITH 1)

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
  ('sensitive' ='true', 'delta.enableDeletionVectors' ='false') --table properties

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('Apple','1976-04-01','USA'),
('Microsoft','1975-04-04','USA'),
('Google','1998-09-04','USA')


-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------


