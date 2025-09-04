
-- models/gold/gold_inventory_status.sql
{{ config(materialized='table') }}
-- Purpose: Report current stock per (store, product) and summarize lifetime stock movements.
--          Sources: silver_inventory (current on-hand), silver_stock_movements (movement facts),
--                   stg_stores (store attributes), stg_products (product attributes).

-- Step 1 — Pre-aggregate stock movements
-- Purpose: collapse the movements to one row per (store_id, product_id) with IN/OUT totals.
WITH sm AS (
    SELECT
        store_id,
        product_id,
        SUM(CASE WHEN movement_type = 'IN'  THEN quantity ELSE 0 END) AS stock_in,
        SUM(CASE WHEN movement_type = 'OUT' THEN quantity ELSE 0 END) AS stock_out
    FROM {{ ref('silver_stock_movements') }}
    GROUP BY store_id, product_id
)

-- Step 2 — Select the final gold columns
-- Purpose: expose business-friendly names and combine facts + dimensions.
SELECT
    i.store_id,                       -- key
    s.store_name,                     -- from stg_stores
    i.product_id,                     -- key
    p.product_name,                   -- from stg_products
    i.quantity AS current_stock,      -- on-hand qty from silver_inventory
    COALESCE(sm.stock_in, 0)  AS stock_in,   -- total received
    COALESCE(sm.stock_out, 0) AS stock_out   -- total issued
-- Step 3 — Base table
-- Purpose: use validated inventory as the base to guarantee one row per (store, product).
FROM {{ ref('silver_inventory') }} AS i
-- Step 4 — Join movement aggregates
-- Purpose: bring IN/OUT totals; left join so products with no movements still appear.
LEFT JOIN sm
  ON sm.store_id   = i.store_id
 AND sm.product_id = i.product_id
-- Step 5 — Join store and product attributes
-- Purpose: enrich with readable names from staging dimensions.
LEFT JOIN {{ ref('silver_stores') }}   AS s ON s.store_id   = i.store_id
LEFT JOIN {{ ref('silver_products') }} AS p ON p.product_id = i.product_id
