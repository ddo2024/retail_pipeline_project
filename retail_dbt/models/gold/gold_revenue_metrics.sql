-- models/gold/gold_revenue_metrics.sql
{{ config(materialized='table') }}
-- Purpose: Calculate revenue per (transaction_date, store, category).
--          Sources: silver_sales_transactions (dates, store), silver_sales_items (qty, unit price),
--                   stg_products (category mapping), stg_categories (category names), stg_stores (store names).

-- Step 1 — Build item-level revenue joined to transaction context
-- Purpose: combine items with their parent transaction to get date + store and compute revenue.
WITH item_txn AS (
    SELECT
        st.transaction_date,
        st.store_id,
        si.product_id,
        (si.quantity * si.unit_price) AS line_revenue
    FROM {{ ref('silver_sales_transactions') }} AS st
    JOIN {{ ref('silver_sales_items') }}        AS si
      ON st.transaction_id = si.transaction_id
),

-- Step 2 — Map products to categories
-- Purpose: attach category_id to each product, then bring readable category_name.
prod_cat AS (
    SELECT
        p.product_id,
        p.category_id
    FROM {{ ref('silver_products') }} AS p
),

cats AS (
    SELECT
        c.category_id,
        c.category_name
    FROM {{ ref('silver_categories') }} AS c
),

stores AS (
    SELECT
        s.store_id,
        s.store_name
    FROM {{ ref('silver_stores') }} AS s
)

-- Step 3 — Final aggregate
-- Purpose: roll up revenue at the reporting grain and expose friendly names.
SELECT
    it.transaction_date,
    st.store_id,
    st.store_name,
    pc.category_id,
    ct.category_name,
    SUM(it.line_revenue) AS revenue
FROM item_txn it
LEFT JOIN prod_cat pc ON pc.product_id = it.product_id
LEFT JOIN cats     ct ON ct.category_id = pc.category_id
LEFT JOIN stores   st ON st.store_id    = it.store_id
GROUP BY
    it.transaction_date,
    st.store_id, st.store_name,
    pc.category_id, ct.category_name
