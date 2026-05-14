-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### COPY INTO AND MERGE Commands
-- MAGIC
-- MAGIC ##### COPY INTO Command
-- MAGIC - Incrementally loads data into delta lake tables from cloud storage
-- MAGIC - Supports Schema evolution
-- MAGIC - Supports wide range of file formats(csv, json, parquet, delta)
-- MAGIC - Alternative to Auto loader for batch ingestion
-- MAGIC - If lower frequency than heavy continuous streaming, COPY INTO can be used. 
-- MAGIC - But best practice for higher frequency continuos stream is Autoloader(i.e spark.readstream.options('cloudFiles))

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ##### Create the table to copy the data into

-- COMMAND ----------

--As COPY INTO supports schema evolution, you can create an empty table without specifying schema.thne when data is read using COPY INTO, it would automatically read the schema and load into df.

-- COMMAND ----------

CREATE TABLE demo.delta_lake.raw_stock_prices

-- COMMAND ----------

COPY INTO demo.delta_lake.raw_stock_prices
FROM 'abfss://demo@dbccourseextdl.dfs.core.windows.net/landing/stock_prices'
FILEFORMAT = JSON
FORMAT_OPTIONS("mergeSchema" ='true' , 'inferSchema' ='true')  --merge differences between source files.
COPY_OPTIONS('mergeSchema'='true') --merges result into the target table




-- COMMAND ----------

-- MAGIC %md
-- MAGIC inferSchema='true' in FORMAT_OPTIONS
-- MAGIC What it does: Automatically detects column names and data types from JSON files
-- MAGIC
-- MAGIC With inferSchema='true' (your setting):
-- MAGIC
-- MAGIC Empty table created → COPY INTO reads JSON → Discovers columns: price, status, stock_id, trading_date → Creates those columns automatically
-- MAGIC Without inferSchema='true':
-- MAGIC
-- MAGIC Empty table created → COPY INTO reads JSON → Fails with error because it doesn't know what columns exist in the files
-- MAGIC You'd need to manually define the table schema: CREATE TABLE ... (price DOUBLE, status STRING, ...)
-- MAGIC mergeSchema='true' in FORMAT_OPTIONS vs COPY_OPTIONS
-- MAGIC These work at different stages:
-- MAGIC
-- MAGIC FORMAT_OPTIONS('mergeSchema'='true')
-- MAGIC Merges schemas between source files (file-to-file)
-- MAGIC
-- MAGIC Example:
-- MAGIC File1.json has: stock_id, price, status
-- MAGIC File2.json has: stock_id, price, status, volume
-- MAGIC Result: Combined schema = stock_id, price, status, volume (File1 gets NULL for volume)
-- MAGIC COPY_OPTIONS('mergeSchema'='true')
-- MAGIC Merges schema from source into target table (DataFrame-to-table)
-- MAGIC
-- MAGIC Example:
-- MAGIC Existing table has: stock_id, price, status
-- MAGIC New files bring: stock_id, price, status, volume
-- MAGIC Result: Table schema updated to add volume column
-- MAGIC Together: FORMAT_OPTIONS merges all source files into one schema → COPY_OPTIONS merges that unified schema into your Delta table.

-- COMMAND ----------

SELECT * FROM demo.delta_lake.raw_stock_prices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### MERGE Statement
-- MAGIC
-- MAGIC - Used for upserts(insert/update/delete operations in a single statement)
-- MAGIC - Allows merging data into a target table based on a match condition.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create the table to merge the data into

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### MERGE THE SOURCE DATA INTO TARGET TABLE
-- MAGIC
-- MAGIC 1.  Insert new stocks received
-- MAGIC 2. Update price and trading_date if updates received
-- MAGIC 3. Delete stocks which are de-listed from the exchange (status='DELISTED')

-- COMMAND ----------

CREATE OR REPLACE TABLE demo.delta_lake.updated_stock_prices
(
    stock_id STRING,
    trading_date DATE,
    price DOUBLE

)

-- COMMAND ----------

SELECT * FROM demo.delta_lake.updated_stock_prices


-- COMMAND ----------

SELECT * FROM demo.delta_lake.raw_stock_prices

-- COMMAND ----------

MERGE INTO demo.delta_lake.updated_stock_prices as tgt --target (empty table to begin with)
USING demo.delta_lake.raw_stock_prices as src --source
ON src.stock_id = tgt.stock_id
WHEN MATCHED AND src.status ='ACTIVE' 
THEN UPDATE SET tgt.price =src.price , tgt.trading_date =src.trading_date
WHEN MATCHED AND src.status ='DELISTED'
THEN DELETE
WHEN NOT MATCHED AND src.status='ACTIVE'
THEN INSERT 
(stock_id, trading_date, price)
VALUES 
(src.stock_id, src.trading_date, src.price)



-- COMMAND ----------

SELECT * FROM demo.delta_lake.updated_stock_prices

-- COMMAND ----------

--LOADING THE new source data into source table .overwriting first

DELETE FROM demo.delta_lake.raw_stock_prices;

COPY INTO demo.delta_lake.raw_stock_prices
FROM 'abfss://demo@dbccourseextdl.dfs.core.windows.net/landing/stock_prices/stock_prices_day3.json'
FILEFORMAT =JSON
FORMAT_OPTIONS('mergeSchema' ='True', 'inferSchema' ='True')
COPY_OPTIONS('mergeSchema' ='True')

-- COMMAND ----------

SELECT * FROM demo.delta_lake.raw_stock_prices

-- COMMAND ----------

--NOW redoing the MERGE Statement with updated source table

MERGE INTO demo.delta_lake.updated_stock_prices as tgt --target
USING demo.delta_lake.raw_stock_prices as src --source
ON src.stock_id = tgt.stock_id
WHEN MATCHED AND src.status ='ACTIVE' 
THEN UPDATE SET tgt.price =src.price , tgt.trading_date =src.trading_date
WHEN MATCHED AND src.status ='DELISTED'
THEN DELETE
WHEN NOT MATCHED AND src.status='ACTIVE'
THEN INSERT 
(stock_id, trading_date, price)
VALUES 
(src.stock_id, src.trading_date, src.price)



-- COMMAND ----------

SELECT * FROM  demo.delta_lake.updated_stock_prices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
