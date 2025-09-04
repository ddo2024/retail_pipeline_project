-- models/gold/gold_daily_sales.sql
-- Purpose: Build a Golden model that reports total sales per day per store.
--          This model reads cleaned facts from Silver and enriches them with
--          the store dimension from Staging (stg_stores).

-- Step 1 — SELECT: choose the analysis grain and measures
-- Purpose: define the columns we want to report at the gold layer.
SELECT
    st.transaction_date,      -- Grain (by day)
    st.store_id,              -- Grain (by store)
    s.store_name,             -- Enrichment from stg_stores
    SUM(st.total_amount) AS total_sales  -- Measure to aggregate

-- Step 2 — FROM: reference the cleaned sales fact table in Silver
-- Purpose: pull validated transactions as the base dataset.
FROM {{ ref('silver_sales_transactions') }} AS st

-- Step 3 — JOIN: bring in store attributes from the staging layer
-- Purpose: attach human-readable store_name from the store dimension.
LEFT JOIN {{ ref('silver_stores') }} AS s
    ON st.store_id = s.store_id

-- ensure metrics are rolled up by (date, store).
GROUP BY st.transaction_date, st.store_id, s.store_name
