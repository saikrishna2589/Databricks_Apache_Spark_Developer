-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![image_1773741439724.png](./image_1773741439724.png "image_1773741439724.png")

-- COMMAND ----------

--create  bronze DLT table from orders data that is incremental. 
--add 2 columns to tbe data set
--read new files in volume using autoloader
--add 2 columns metadata filename and timestamp
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'bronze - orders data table'
TBLPROPERTIES ('quality' = 'bronze')
AS

SELECT * , _metadata.file_path as input_file_path,
current_timestamp() as ingestion_timestamp
FROM cloud_files('/Volumes/circuitbox/landing/operational_data/orders',
                  'json', 
                   map('cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_orders_clean
--data quality expectations

(

  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)
  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)
  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_status EXPECT (order_status NOT IN
  ('Pending','Shipped','Cancelled','Completed') ),
CONSTRAINT valid_order_status EXPECT (order_status NOT IN
  ('Bank Transfer','PayPal','Cancelled','Completed') )
)
COMMENT 'silver_cleaned'
TBLPROPERTIES ('quality' ='silver')

AS

SELECT order_id,
customer_id,
CAST(order_timestamp AS TIMESTAMP) as order_timestamp
payment_method,
items,
order_status
FROM STREAM(LIVE.bronze_orders)



-- COMMAND ----------

--now lets go Silver table
CREATE OR REFRESH STREAMING TABLE silver_orders
AS
SELECT order_id,
customer_id,
order_timestamp,
payment_method,
order_status,
item.item_id,
item.
explode(items)

