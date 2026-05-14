-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Insert Overwrite
-- MAGIC  1. Replace all the data in a table
-- MAGIC  2. Replace all the data from a  specific partition
-- MAGIC  3. How to handle schema changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC INSERT OVERWRITE - Overwrites the existing data in the table or a specific partition with the new data.
-- MAGIC
-- MAGIC INSERT INTO -Appends new data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Replace all the data in the data

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;

-- COMMAND ----------


INSERT OVERWRITE  demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('Apple','1976-04-01','USA'),
('Microsoft','1975-04-04','USA'),
('Google','1998-09-04','USA'),
('Indiafocus','2027-01-01','India')



-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

--INSERT INTO WITH PARTITION -JUST CHANGE ONLY THAT SPECIFIC ROW

-- COMMAND ----------

-- DBTITLE 1,Cell 8
--FIRST LETS PARTITION THE TABLE
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
  ('sensitive' ='true', 'delta.enableDeletionVectors' ='false')  --table properties;
  PARTITIONED BY (country);


INSERT INTO demo.delta_lake.companies
(company_name, founded_date,country)
VALUES
('Apple','1976-04-01','USA'),
('Microsoft','1975-04-04','USA'),
('Google','1998-09-04','USA'),
('Indiafocus','2027-01-01','India'),
('BYD' ,'1995-02-01','China')

-- COMMAND ----------

-- DBTITLE 1,Untitled
--CHANGE SPECIFIC PARTIION row value. 
INSERT OVERWRITE demo.delta_lake.companies
PARTITION (country ='China') --partition clause

--(company_name, founded_date) --column list
--VALUES
 --('BYD','1995-02-10')


-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

ALTER TABLE demo.delta_lake.companies 
SET 

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.delta_lake.companies

-- COMMAND ----------

--if 2 tables are there and you want to replace data from one table into another table,
--then you can use INSERT OVERWRITE. It deletes the target table data and overwrites with source table data.

--for overwriting entire table.

INSERT OVERWRITE demo.delta_lake.gold_companies --target table
SELECT * FROM demo.delta_lake.bronze_companies  --query result from source table

-- COMMAND ----------


--then you can check history for INSERT OVERWRITE statement 
DESCRIBE HISTORY demo.delta_lake.gold_companies 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### How to handle schema changes with Insert Overwrite
-- MAGIC
-- MAGIC -USE CREATE or REPLACE TABLE -->When there are schema changes and then followed by INSERT 
-- MAGIC
-- MAGIC INSERT OVERWRITE catalog.schema.table_name
-- MAGIC PARTITION(country ='USA)
-- MAGIC SELECT company_name , founded_date
-- MAGIC FROM table_usa_records
-- MAGIC
