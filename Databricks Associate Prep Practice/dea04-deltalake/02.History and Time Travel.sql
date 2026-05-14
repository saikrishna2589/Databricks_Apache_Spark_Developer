-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### History and Time Travel
-- MAGIC 1. Query Delta Lake table histrory
-- MAGIC 2. Query Previous versions of the data
-- MAGIC 3. Query data from a specific time
-- MAGIC 4. Restore data to a specific version
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
VERSION AS OF 1
--timestamp as of '2026-03-03T02:52:26.000+00:00'

-- COMMAND ----------

RESTORE demo.delta_lake.companies  VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies

-- COMMAND ----------

DESCRIBE HISTORY  demo.delta_lake.companies

-- COMMAND ----------


