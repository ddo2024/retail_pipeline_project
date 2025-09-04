-- models/gold/gold_return_rates.sql
{{ config(materialized='table') }}
-- Purpose: Calculate return percentage by (product, store).
--          Sources: silver_sales_items (items sold), silver_sales_transactions (store mapping),
--                   silver_returns (returned items), stg_products, stg_stores.

-- Step 1 — Build a sales base at item grain
-- Purpose: get (item_id, product_id, store_id) from cleaned silver facts.
WITH sales_base AS (
    SELECT
        si.item_id,
        si.product_id,
        st.store_id
    FROM {{ ref('stg_sales_items') }}        AS si
    JOIN {{ ref('silver_sales_transactions') }} AS st
      ON si.transaction_id = st.transaction_id
),

-- Step 2 — Aggregate denominator: total items sold per (product, store)
-- Purpose: count distinct item rows to use as the denominator for return rate.
sold AS (
    SELECT
        product_id,
        store_id,
        COUNT(DISTINCT item_id) AS sold_items
    FROM sales_base
    GROUP BY product_id, store_id
),

-- Step 3 — Aggregate numerator: returned items per (product, store)
-- Purpose: count returns linked to sold items to use as the numerator.
ret AS (
    SELECT
        sb.product_id,
        sb.store_id,
        COUNT(r.return_id) AS returned_items
    FROM sales_base sb
    LEFT JOIN {{ ref('silver_returns') }} AS r
      ON sb.item_id = r.item_id
    GROUP BY sb.product_id, sb.store_id
)

-- Step 4 — Final select with dimensional attributes and safe percentage
-- Purpose: join to product/store names; compute percentage with zero-safe division.
SELECT
    p.product_id,
    p.product_name,
    s.store_id,
    s.store_name,
    COALESCE(ret.returned_items, 0) AS returned_items,
    COALESCE(sold.sold_items, 0)    AS sold_items,
    CASE
        WHEN COALESCE(sold.sold_items, 0) = 0 THEN 0.0
        ELSE ROUND(COALESCE(ret.returned_items, 0) * 100.0 / sold.sold_items, 2)
    END AS return_percentage
FROM sold
LEFT JOIN ret
  ON ret.product_id = sold.product_id AND ret.store_id = sold.store_id
LEFT JOIN {{ ref('silver_products') }} AS p
  ON p.product_id = sold.product_id
LEFT JOIN {{ ref('silver_stores') }} AS s
  ON s.store_id = sold.store_id
