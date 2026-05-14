# Databricks notebook source
# DBTITLE 1,Why Materialized Views for Gold Layer
# MAGIC %md
# MAGIC ## Why Materialized Views for the Gold Layer?
# MAGIC
# MAGIC ### What is a Materialized View?
# MAGIC A **materialized view** is a query whose results are **precomputed and stored** as a physical table. Unlike a regular view (which re-runs the query every time), a materialized view caches the results — giving you **fast reads** while still being tied to its source query.
# MAGIC
# MAGIC ### Why Gold Layer = Materialized View (not Streaming Table)?
# MAGIC
# MAGIC | Aspect | Streaming Table | Materialized View |
# MAGIC | --- | --- | --- |
# MAGIC | **Semantics** | Append-only, streaming | Batch, full recompute or incremental |
# MAGIC | **Best for** | Bronze / Silver (raw ingestion, cleaning) | Gold (aggregations, joins, business KPIs) |
# MAGIC | **Handles updates/deletes upstream?** | No — append only | Yes — automatically recomputes |
# MAGIC | **Use case** | Ingesting new rows continuously | Summarising, aggregating, joining |
# MAGIC
# MAGIC **Gold layer tables are typically aggregations** (e.g., daily revenue, customer lifetime value). When upstream silver data gets corrected, updated, or deleted:
# MAGIC - A **materialized view** automatically recomputes to reflect the change — **always correct**
# MAGIC - A **streaming table** would NOT handle upstream corrections (it only appends)
# MAGIC
# MAGIC ### So Should It Ever Be a Table (Streaming Table)?
# MAGIC Yes! Use a **streaming table** at gold if:
# MAGIC - Your gold table is **append-only** (e.g., an event log, audit trail)
# MAGIC - You need **low-latency incremental** processing with no upstream corrections
# MAGIC - You never update or delete upstream silver data
# MAGIC
# MAGIC **Rule of thumb:** If the gold query has `GROUP BY`, `JOIN`, or `WINDOW` functions → **materialized view**. If it's a simple filter/pass-through on a stream → streaming table is fine.

# COMMAND ----------

# DBTITLE 1,Real-World Example Intro
# MAGIC %md
# MAGIC ## Real-World Example: E-Commerce Order Pipeline (DLT)
# MAGIC
# MAGIC Scenario: An online retailer ingests order data and needs:
# MAGIC - **Bronze**: Raw orders from cloud storage (streaming table)
# MAGIC - **Silver**: Cleaned & validated orders (streaming table)
# MAGIC - **Gold**: Daily revenue by product category (materialized view) — used by dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why this example uses materialized views at gold:
# MAGIC - If a silver order gets **corrected** (e.g., wrong price fixed), the gold daily revenue **auto-recomputes**
# MAGIC - If an order is **deleted** (e.g., GDPR request), the gold aggregation **drops it automatically**
# MAGIC - A streaming table at gold would **never fix** these — it only appends

# COMMAND ----------

# DBTITLE 1,Bronze Layer — Streaming Table (Raw Ingestion)
# ---------------------------------------------------------------
# BRONZE LAYER — Streaming Table (raw ingestion, append-only)
# ---------------------------------------------------------------
# This is a STREAMING TABLE because we're ingesting raw data
# from cloud storage. New files land → new rows appended.
# ---------------------------------------------------------------

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_orders",
    comment="Raw e-commerce orders ingested from cloud storage"
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")            # Auto Loader
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/mnt/landing/orders/")    # raw JSON files land here
            .select(
                "*",
                current_timestamp().alias("_ingested_at"),
                input_file_name().alias("_source_file")
            )
    )

# COMMAND ----------

# DBTITLE 1,Silver Layer — Streaming Table (Cleaned & Validated)
# ---------------------------------------------------------------
# SILVER LAYER — Streaming Table (cleaned & validated)
# ---------------------------------------------------------------
# Still a STREAMING TABLE because we're processing the bronze
# stream incrementally. We clean, deduplicate, and validate.
# ---------------------------------------------------------------

@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated orders"
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_price", "unit_price > 0")
@dlt.expect_or_drop("not_null_order_id", "order_id IS NOT NULL")
def silver_orders():
    return (
        dlt.readStream("bronze_orders")
            .select(
                col("order_id").cast("long"),
                col("customer_id").cast("long"),
                col("product_category").cast("string"),
                col("product_name").cast("string"),
                col("quantity").cast("int"),
                col("unit_price").cast("double"),
                (col("quantity") * col("unit_price")).alias("total_amount"),
                to_date(col("order_date")).alias("order_date"),
                col("region").cast("string"),
                col("_ingested_at")
            )
    )

# COMMAND ----------

# DBTITLE 1,Gold Layer — Materialized View (Daily Revenue)
# ---------------------------------------------------------------
# GOLD LAYER — MATERIALIZED VIEW (aggregated business KPIs)
# ---------------------------------------------------------------
# THIS IS THE KEY PART: We use a MATERIALIZED VIEW here because:
#
# 1. It's an AGGREGATION (GROUP BY) — not append-only
# 2. If upstream silver data is corrected → gold auto-recomputes
# 3. If data is deleted (GDPR) → gold reflects it after refresh
# 4. Dashboards read from this → always see correct, latest totals
#
# If this were a streaming table, corrections/deletes would be LOST.
# ---------------------------------------------------------------

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue by product category — used by exec dashboards"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")          # <-- batch read, NOT readStream!
            .groupBy("order_date", "product_category", "region")
            .agg(
                sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("total_amount").alias("avg_order_value")
            )
    )

# COMMAND ----------

# DBTITLE 1,Gold Layer — Materialized View (Customer LTV)
# ---------------------------------------------------------------
# GOLD LAYER — Another Materialized View (Customer Lifetime Value)
# ---------------------------------------------------------------
# Another great example of why materialized views:
# Customer metrics change when orders are corrected or removed.
# ---------------------------------------------------------------

@dlt.table(
    name="gold_customer_lifetime_value",
    comment="Customer lifetime value metrics"
)
def gold_customer_lifetime_value():
    return (
        dlt.read("silver_orders")
            .groupBy("customer_id")
            .agg(
                sum("total_amount").alias("lifetime_spend"),
                count("order_id").alias("total_orders"),
                min("order_date").alias("first_order_date"),
                max("order_date").alias("last_order_date"),
                countDistinct("product_category").alias("categories_purchased")
            )
    )

# COMMAND ----------

# DBTITLE 1,Under the Hood — How It Works
# MAGIC %md
# MAGIC ## What Happens Under the Hood
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐     ┌─────────────────────┐     ┌──────────────────────────┐
# MAGIC │  BRONZE (Stream)    │────▶│  SILVER (Stream)    │────▶│  GOLD (Materialized View)│
# MAGIC │                     │     │                     │     │                          │
# MAGIC │  Streaming Table    │     │  Streaming Table    │     │  dlt.read() = batch read │
# MAGIC │  dlt.readStream()   │     │  dlt.readStream()   │     │  GROUP BY aggregations   │
# MAGIC │  Append-only        │     │  Append-only        │     │  Auto-recomputes on      │
# MAGIC │  New files → new    │     │  Clean + validate   │     │  refresh if upstream     │
# MAGIC │  rows               │     │  + expectations     │     │  data changed            │
# MAGIC └─────────────────────┘     └─────────────────────┘     └──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Observation: `dlt.read()` vs `dlt.readStream()`
# MAGIC
# MAGIC | Function | Used In | Behaviour |
# MAGIC | --- | --- | --- |
# MAGIC | `dlt.readStream()` | Bronze → Silver | Incremental / streaming (append-only) |
# MAGIC | `dlt.read()` | Silver → Gold | **Batch read** — reads ALL silver data, recomputes gold from scratch (or incrementally if possible) |
# MAGIC
# MAGIC This is **why** gold is a materialized view — `dlt.read()` triggers batch/full recomputation semantics, which is what materialized views do.
# MAGIC
# MAGIC ### Summary Decision Guide
# MAGIC
# MAGIC | Layer | Object Type | Why |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | Streaming Table | Append-only raw ingestion |
# MAGIC | Silver | Streaming Table | Incremental cleaning of new rows |
# MAGIC | Gold (aggregations) | **Materialized View** | Recomputes on refresh, handles upstream changes |
# MAGIC | Gold (append-only log) | Streaming Table | If gold is just filtering a stream with no aggregation |

# COMMAND ----------

# DBTITLE 1,SQL Approach — Same Pipeline in DLT SQL
# MAGIC %md
# MAGIC ## Same Pipeline in DLT SQL
# MAGIC
# MAGIC Everything above can be written **entirely in SQL** using DLT SQL syntax.  
# MAGIC Below is the **exact same** Bronze → Silver → Gold pipeline — same logic, same data, same outcome.
# MAGIC
# MAGIC > **Key difference:** In SQL, you explicitly declare `STREAMING TABLE` or `MATERIALIZED VIEW` — there's no decorator.  
# MAGIC > In Python, DLT infers the type from whether you use `dlt.readStream()` (→ streaming table) or `dlt.read()` (→ materialized view).
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,SQL — Bronze Layer (Streaming Table)
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- BRONZE LAYER (SQL) — Streaming Table (raw ingestion)
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- STREAMING TABLE: appends new rows as new JSON files land.
# MAGIC -- cloud_files() is the SQL equivalent of Auto Loader.
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC COMMENT 'Raw e-commerce orders ingested from cloud storage (SQL)'
# MAGIC AS SELECT
# MAGIC   *,
# MAGIC   current_timestamp()  AS _ingested_at,
# MAGIC   input_file_name()    AS _source_file
# MAGIC FROM cloud_files(
# MAGIC   '/mnt/landing/orders/',
# MAGIC   'json',
# MAGIC   map('cloudFiles.inferColumnTypes', 'true')
# MAGIC );

# COMMAND ----------

# DBTITLE 1,SQL — Silver Layer (Streaming Table with Expectations)
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- SILVER LAYER (SQL) — Streaming Table (cleaned & validated)
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- Still a STREAMING TABLE — processes bronze stream incrementally.
# MAGIC -- CONSTRAINT ... EXPECT is the SQL equivalent of @dlt.expect_or_drop
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders (
# MAGIC   CONSTRAINT valid_quantity   EXPECT (quantity > 0)       ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_price      EXPECT (unit_price > 0)     ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT not_null_order   EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Cleaned and validated orders (SQL)'
# MAGIC AS SELECT
# MAGIC   CAST(order_id         AS BIGINT)  AS order_id,
# MAGIC   CAST(customer_id      AS BIGINT)  AS customer_id,
# MAGIC   CAST(product_category AS STRING)  AS product_category,
# MAGIC   CAST(product_name     AS STRING)  AS product_name,
# MAGIC   CAST(quantity         AS INT)     AS quantity,
# MAGIC   CAST(unit_price       AS DOUBLE)  AS unit_price,
# MAGIC   quantity * unit_price             AS total_amount,
# MAGIC   TO_DATE(order_date)               AS order_date,
# MAGIC   CAST(region           AS STRING)  AS region,
# MAGIC   _ingested_at
# MAGIC FROM STREAM(LIVE.bronze_orders);

# COMMAND ----------

# DBTITLE 1,SQL — Gold Layer: Daily Revenue (Materialized View)
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- GOLD LAYER (SQL) — MATERIALIZED VIEW (daily revenue KPIs)
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- THIS is the key: CREATE OR REFRESH MATERIALIZED VIEW
# MAGIC --
# MAGIC -- 1. No STREAM() — reads silver as a batch (full recompute)
# MAGIC -- 2. GROUP BY = aggregation → materialized view is correct choice
# MAGIC -- 3. Upstream corrections/deletes → auto-reflected after refresh
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
# MAGIC COMMENT 'Daily revenue by product category — used by exec dashboards (SQL)'
# MAGIC AS SELECT
# MAGIC   order_date,
# MAGIC   product_category,
# MAGIC   region,
# MAGIC   SUM(total_amount)    AS total_revenue,
# MAGIC   COUNT(order_id)      AS order_count,
# MAGIC   AVG(total_amount)    AS avg_order_value
# MAGIC FROM LIVE.silver_orders
# MAGIC GROUP BY order_date, product_category, region;

# COMMAND ----------

# DBTITLE 1,SQL — Gold Layer: Customer Lifetime Value (Materialized View)
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- GOLD LAYER (SQL) — MATERIALIZED VIEW (Customer Lifetime Value)
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- Same pattern: batch read + aggregation = materialized view.
# MAGIC -- If a customer's order is corrected or deleted upstream,
# MAGIC -- this view auto-recomputes on the next pipeline refresh.
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_customer_lifetime_value
# MAGIC COMMENT 'Customer lifetime value metrics (SQL)'
# MAGIC AS SELECT
# MAGIC   customer_id,
# MAGIC   SUM(total_amount)                    AS lifetime_spend,
# MAGIC   COUNT(order_id)                      AS total_orders,
# MAGIC   MIN(order_date)                      AS first_order_date,
# MAGIC   MAX(order_date)                      AS last_order_date,
# MAGIC   COUNT(DISTINCT product_category)     AS categories_purchased
# MAGIC FROM LIVE.silver_orders
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# DBTITLE 1,Python vs SQL — Side-by-Side Comparison
# MAGIC %md
# MAGIC ## Python vs SQL — Side-by-Side Comparison
# MAGIC
# MAGIC | Concept | Python DLT | SQL DLT |
# MAGIC | --- | --- | --- |
# MAGIC | **Streaming table** | `@dlt.table` + `dlt.readStream()` | `CREATE OR REFRESH STREAMING TABLE` + `STREAM(LIVE.x)` |
# MAGIC | **Materialized view** | `@dlt.table` + `dlt.read()` | `CREATE OR REFRESH MATERIALIZED VIEW` |
# MAGIC | **Auto Loader** | `spark.readStream.format("cloudFiles")` | `cloud_files('/path', 'json')` |
# MAGIC | **Data quality** | `@dlt.expect_or_drop("name", "expr")` | `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW` |
# MAGIC | **Read silver (batch)** | `dlt.read("silver_orders")` | `LIVE.silver_orders` (no STREAM wrapper) |
# MAGIC | **Read silver (stream)** | `dlt.readStream("silver_orders")` | `STREAM(LIVE.silver_orders)` |
# MAGIC
# MAGIC ### Key Takeaway
# MAGIC
# MAGIC In **SQL**, the distinction is **explicit** in the DDL:
# MAGIC - `CREATE OR REFRESH STREAMING TABLE` → streaming table
# MAGIC - `CREATE OR REFRESH MATERIALIZED VIEW` → materialized view
# MAGIC
# MAGIC In **Python**, the distinction is **implicit** in what you read:
# MAGIC - `dlt.readStream()` → streaming table
# MAGIC - `dlt.read()` → materialized view
# MAGIC
# MAGIC > **Both produce identical results.** Choose based on team preference — SQL is more readable for analysts, Python gives more flexibility for complex transformations.
