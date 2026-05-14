# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Customer Data
# MAGIC 1. Remove records with NULL customer_id
# MAGIC 2. Remove exact duplicate records
# MAGIC 3. Remove records based on created_timestamp
# MAGIC 4. Cast the columns to the correct Data Type
# MAGIC 5. Write transformed data to the silver schema.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Remove records with NULL customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Remove exact duplicate records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC ORDER BY customer_Id

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Remove duplicate records based on the timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * 
# MAGIC FROM gizmobox.bronze.v_customers
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC AND created_timestamp = (SELECT MAX(created_timestamp) FROM gizmobox.bronze.v_customers c2 WHERE c2.customer_id = v_customers.customer_id)
# MAGIC ORDER BY customer_Id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_distinct_records
# MAGIC AS
# MAGIC
# MAGIC SELECT DISTINCT *
# MAGIC FROM gizmobox.bronze.v_customers AS v1
# MAGIC WHERE v1.created_timestamp =
# MAGIC  (SELECT MAX(v2.created_timestamp) 
# MAGIC  FROM gizmobox.bronze.v_customers AS v2
# MAGIC   WHERE v2.customer_Id = v1.customer_id)
# MAGIC AND customer_id IS NOT NULL
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_distinct_records

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Cast the column values to the correct datatype

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC WITH v_distinct_records
# MAGIC   (
# MAGIC   SELECT DISTINCT *
# MAGIC   FROM gizmobox.bronze.v_customers AS v1
# MAGIC   WHERE v1.created_timestamp =
# MAGIC   (SELECT MAX(v2.created_timestamp) 
# MAGIC   FROM gizmobox.bronze.v_customers AS v2
# MAGIC     WHERE v2.customer_Id = v1.customer_id)
# MAGIC   AND customer_id IS NOT NULL
# MAGIC   ORDER BY customer_id
# MAGIC   )
# MAGIC
# MAGIC SELECT CAST(created_timestamp AS timestamp) AS created_timestamp ,
# MAGIC customer_id,
# MAGIC email,
# MAGIC CAST(date_of_birth AS DATE),
# MAGIC CAST(member_since AS DATE) AS member_since,
# MAGIC telephone
# MAGIC --to_timestamp(created_timestamp, 'yyyy-MM-dd HH:mm:ss') AS  created_timestamp_custom
# MAGIC FROM v_distinct_records

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Store the transformed cleansed data in Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --storing the cleansed and deduplicated data in a silver table.
# MAGIC CREATE TABLE gizmobox.silver.customers
# MAGIC AS
# MAGIC
# MAGIC --deduplicating records as well as choosing latest timestmap record by customer_id if customer_id is the same.
# MAGIC     WITH v_distinct_records
# MAGIC       (
# MAGIC       SELECT DISTINCT *
# MAGIC       FROM gizmobox.bronze.v_customers AS v1
# MAGIC       WHERE v1.created_timestamp =
# MAGIC       (SELECT MAX(v2.created_timestamp) 
# MAGIC       FROM gizmobox.bronze.v_customers AS v2
# MAGIC         WHERE v2.customer_Id = v1.customer_id)
# MAGIC       AND customer_id IS NOT NULL
# MAGIC       ORDER BY customer_id
# MAGIC       )
# MAGIC
# MAGIC --casting the columns into timestamp and date 
# MAGIC     SELECT CAST(created_timestamp AS timestamp) AS created_timestamp ,
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     CAST(date_of_birth AS DATE),
# MAGIC     CAST(member_since AS DATE) AS member_since,
# MAGIC     telephone
# MAGIC     --to_timestamp(created_timestamp, 'yyyy-MM-dd HH:mm:ss') AS  created_timestamp_custom
# MAGIC     FROM v_distinct_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Understanding How the Subquery Deduplication Works
# MAGIC
# MAGIC **The Problem:** Multiple records exist for the same customer_id with different timestamps. We want to keep only the most recent record.
# MAGIC
# MAGIC **The Solution:** Use a correlated subquery that finds the MAX timestamp for each customer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Example Input Data:**
# MAGIC
# MAGIC | created_timestamp | customer_id | customer_name | email |
# MAGIC |-------------------|-------------|---------------|-------|
# MAGIC | 2024-01-01 10:00 | 101 | John Doe | john@email.com |
# MAGIC | 2024-01-05 14:30 | 101 | John Doe | john@email.com |
# MAGIC | 2024-01-03 09:15 | 101 | John Doe | john@email.com |
# MAGIC | 2024-01-02 11:00 | 202 | Jane Smith | jane@email.com |
# MAGIC | 2024-01-06 16:45 | 202 | Jane Smith | jane@email.com |
# MAGIC
# MAGIC **Expected Output (Keep only latest per customer):**
# MAGIC
# MAGIC | created_timestamp | customer_id | customer_name | email |
# MAGIC |-------------------|-------------|---------------|-------|
# MAGIC | 2024-01-05 14:30 | 101 | John Doe | john@email.com |
# MAGIC | 2024-01-06 16:45 | 202 | Jane Smith | jane@email.com |

# COMMAND ----------

# DBTITLE 1,Create Sample Data
# MAGIC %sql
# MAGIC -- Create a temporary view with sample data to demonstrate the concept
# MAGIC CREATE OR REPLACE TEMP VIEW sample_customers AS
# MAGIC SELECT '2024-01-01 10:00:00' AS created_timestamp, 101 AS customer_id, 'John Doe' AS customer_name, 'john@email.com' AS email
# MAGIC UNION ALL
# MAGIC SELECT '2024-01-05 14:30:00', 101, 'John Doe', 'john@email.com'
# MAGIC UNION ALL
# MAGIC SELECT '2024-01-03 09:15:00', 101, 'John Doe', 'john@email.com'
# MAGIC UNION ALL
# MAGIC SELECT '2024-01-02 11:00:00', 202, 'Jane Smith', 'jane@email.com'
# MAGIC UNION ALL
# MAGIC SELECT '2024-01-06 16:45:00', 202, 'Jane Smith', 'jane@email.com';
# MAGIC
# MAGIC SELECT * FROM sample_customers ORDER BY customer_id, created_timestamp

# COMMAND ----------

# DBTITLE 1,Step 1: Find MAX Timestamp Per Customer
# MAGIC %sql
# MAGIC -- STEP 1: First, let's see what the subquery returns
# MAGIC -- This finds the MAXIMUM (most recent) timestamp for each customer
# MAGIC
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   MAX(created_timestamp) AS max_timestamp
# MAGIC FROM sample_customers
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### How the Correlated Subquery Executes
# MAGIC
# MAGIC **The query processes row by row:**
# MAGIC
# MAGIC 1. **For each row in the outer query (v1)**, the subquery runs and finds the MAX timestamp for THAT specific customer_id
# MAGIC 2. **The WHERE clause compares** the current row's timestamp with the MAX timestamp
# MAGIC 3. **Only rows where timestamps match** are kept
# MAGIC
# MAGIC **Visual Example for customer_id = 101:**
# MAGIC
# MAGIC ```
# MAGIC Outer Query Row 1: timestamp='2024-01-01 10:00', customer_id=101
# MAGIC   → Subquery finds: MAX(timestamp) for customer_id=101 = '2024-01-05 14:30'
# MAGIC   → Compare: '2024-01-01 10:00' = '2024-01-05 14:30'? NO → Row excluded
# MAGIC
# MAGIC Outer Query Row 2: timestamp='2024-01-05 14:30', customer_id=101
# MAGIC   → Subquery finds: MAX(timestamp) for customer_id=101 = '2024-01-05 14:30'
# MAGIC   → Compare: '2024-01-05 14:30' = '2024-01-05 14:30'? YES → Row kept ✓
# MAGIC
# MAGIC Outer Query Row 3: timestamp='2024-01-03 09:15', customer_id=101
# MAGIC   → Subquery finds: MAX(timestamp) for customer_id=101 = '2024-01-05 14:30'
# MAGIC   → Compare: '2024-01-03 09:15' = '2024-01-05 14:30'? NO → Row excluded
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 3: Apply the Full Query
# MAGIC %sql
# MAGIC -- STEP 3: Now apply the complete deduplication logic
# MAGIC -- This keeps only rows where the timestamp equals the MAX timestamp for that customer
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   v1.created_timestamp,
# MAGIC   v1.customer_id,
# MAGIC   v1.customer_name, 
# MAGIC   v1.email
# MAGIC FROM sample_customers AS v1
# MAGIC WHERE v1.created_timestamp = (
# MAGIC   SELECT MAX(v2.created_timestamp) 
# MAGIC   FROM sample_customers AS v2
# MAGIC   WHERE v2.customer_id = v1.customer_id  -- This links outer and inner query (CORRELATED)
# MAGIC )
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Alternative Approach: Using Window Functions (Often More Efficient)
# MAGIC
# MAGIC Window functions can be easier to understand and often perform better than correlated subqueries:

# COMMAND ----------

# DBTITLE 1,Window Function Example
# MAGIC %sql
# MAGIC -- Alternative approach using ROW_NUMBER() window function
# MAGIC -- This ranks records within each customer_id partition by timestamp (newest first)
# MAGIC
# MAGIC WITH ranked_customers AS (
# MAGIC   SELECT 
# MAGIC     created_timestamp,
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     email,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_timestamp DESC) AS row_num
# MAGIC   FROM sample_customers
# MAGIC )
# MAGIC SELECT 
# MAGIC   created_timestamp,
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email
# MAGIC FROM ranked_customers
# MAGIC WHERE row_num = 1  -- Keep only the first row (most recent) for each customer
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Concepts Summary
# MAGIC
# MAGIC **Correlated Subquery:**
# MAGIC * **"Correlated"** means the inner query references columns from the outer query (`v2.customer_id = v1.customer_id`)
# MAGIC * The subquery **executes once for each row** in the outer query
# MAGIC * It finds the MAX timestamp for the current row's customer_id
# MAGIC * Only rows matching the MAX timestamp are kept
# MAGIC
# MAGIC **Window Function (Alternative):**
# MAGIC * **PARTITION BY** divides data into groups (one per customer_id)
# MAGIC * **ORDER BY DESC** ranks records within each group (newest first)
# MAGIC * **ROW_NUMBER()** assigns rank 1 to the most recent record
# MAGIC * Filter for `row_num = 1` to keep only the latest
# MAGIC
# MAGIC **Performance Note:** Window functions are typically faster for large datasets because they scan the table once, while correlated subqueries may scan multiple times.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT v1.created_timestamp,v1.customer_id,v1.customer_name, v1.email
# MAGIC FROM gizmobox.bronze.v_customers AS v1
# MAGIC WHERE v1.created_timestamp =
# MAGIC  (SELECT MAX(created_timestamp) 
# MAGIC  FROM gizmobox.bronze.v_customers AS 
# MAGIC   GROUP BY customer_Id
# MAGIC   WHERE customer_id IS NOT NULL)
# MAGIC AND customer_id IS NOT NULL
# MAGIC ORDER BY customer_id
