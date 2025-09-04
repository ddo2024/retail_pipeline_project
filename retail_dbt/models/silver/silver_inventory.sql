-- models/silver/silver_inventory.sql
{{ config(materialized='table') }}

-- STEP 1: CAST + TRIM
-- PURPOSE: Enforce stable types for reliable joins and math
WITH base AS (
  SELECT
    inventory_id::INT  AS inventory_id,
    store_id::INT      AS store_id,
    product_id::INT    AS product_id,
    COALESCE(quantity, 0)::INT AS quantity,
    last_updated::TEXT AS last_updated_raw
  FROM {{ ref('stg_inventory') }}
)

-- STEP 2: FIX DATE STRING (HANDLE '00' MONTH/DAY)
-- PURPOSE: Repair malformed dates like '2030-00-01' -> '2030-01-01' before casting
, date_fixed AS (
  SELECT
    inventory_id, store_id, product_id, quantity,
    CASE
      WHEN last_updated_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CONCAT_WS('-',
          SUBSTRING(last_updated_raw, 1, 4),
          LPAD(COALESCE(NULLIF(SUBSTRING(last_updated_raw, 6, 2), '00'), '01'), 2, '0'),
          LPAD(COALESCE(NULLIF(SUBSTRING(last_updated_raw, 9, 2), '00'), '01'), 2, '0')
        )
      ELSE NULL
    END AS last_updated_fixed_str
  FROM base
)

-- STEP 3: PARSE + FORCE INTO PAST
-- PURPOSE: Convert to DATE and cap any future dates at CURRENT_DATE
, dated AS (
  SELECT
    inventory_id, store_id, product_id, quantity,
    CASE
      WHEN last_updated_fixed_str IS NULL THEN NULL
      WHEN TO_DATE(last_updated_fixed_str, 'YYYY-MM-DD') > CURRENT_DATE THEN CURRENT_DATE
      ELSE TO_DATE(last_updated_fixed_str, 'YYYY-MM-DD')
    END AS last_updated
  FROM date_fixed
)

-- STEP 4: FILTER ESSENTIALS
-- PURPOSE: Keep only usable rows (valid keys and date)
, valid AS (
  SELECT *
  FROM dated
  WHERE store_id IS NOT NULL
    AND product_id IS NOT NULL
    AND last_updated IS NOT NULL
)

-- STEP 5: DEDUP TO LATEST SNAPSHOT
-- PURPOSE: One row per (store_id, product_id) using the most recent last_updated
SELECT DISTINCT ON (store_id, product_id)
  inventory_id,
  store_id,
  product_id,
  GREATEST(quantity, 0) AS quantity,  -- ensure non-negative
  last_updated
FROM valid
ORDER BY store_id, product_id, last_updated DESC NULLS LAST