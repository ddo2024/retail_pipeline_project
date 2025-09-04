-- models/gold/gold_product_performance.sql
{{ config(materialized='table') }}
-- Purpose: Summarize product sales volume and revenue to identify top-selling products.
--          Sources: silver_sales_items (fact), stg_products (product attributes).

-- Step 1 — Aggregate item-level facts
-- Purpose: compute core metrics per product from cleaned silver facts.
WITH item_metrics AS (
    SELECT
        si.product_id,
        SUM(si.quantity)                       AS total_quantity_sold,
        SUM(si.quantity * si.unit_price)       AS total_revenue
    FROM {{ ref('silver_sales_items') }} AS si
    GROUP BY si.product_id
)

-- Step 2 — Join to product dimension
-- Purpose: attach human-readable product_name and any other attributes.
, prod AS (
    SELECT
        p.product_id,
        p.product_name
    FROM {{ ref('silver_products') }} AS p
)

-- Step 3 — Combine + rank
-- Purpose: expose final fields and add ranks to support "top" analysis.
SELECT
    m.product_id,
    pr.product_name,
    m.total_quantity_sold,
    m.total_revenue,
    DENSE_RANK() OVER (ORDER BY m.total_quantity_sold DESC) AS quantity_rank,
    DENSE_RANK() OVER (ORDER BY m.total_revenue DESC)        AS revenue_rank
FROM item_metrics m
LEFT JOIN prod pr
  ON pr.product_id = m.product_id
