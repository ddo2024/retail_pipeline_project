-- models/silver/silver_products.sql
{{ config(materialized='table') }}

-- STEP 1: CAST + BASIC CLEAN
-- PURPOSE: Normalize types, trim text, keep price as TEXT for safe parsing
WITH base AS (
  SELECT
    product_id::INT          AS product_id,
    TRIM(name)               AS name,
    category_id::INT         AS category_id,
    brand_id::INT            AS brand_id,
    supplier_id::INT         AS supplier_id,
    NULLIF(TRIM(price::TEXT), 'N/A') AS price_raw,
    created_at::TEXT         AS created_at_raw,
    TRIM(season)             AS season
  FROM {{ ref('stg_products') }}
),

-- STEP 2: SAFE PRICE CAST
-- PURPOSE: Cast only valid numerics; default invalid/missing to 0.00
price_clean AS (
  SELECT
    product_id, name, category_id, brand_id, supplier_id, season, created_at_raw,
    CASE
      WHEN price_raw ~ '^[0-9]+(\.[0-9]+)?$' THEN price_raw::NUMERIC(18,2)
      ELSE 0.00::NUMERIC(18,2)
    END AS price
  FROM base
),

-- STEP 3: FIX DATE STRING (HANDLE '00' MONTH/DAY)
-- PURPOSE: Repair malformed dates like '2030-00-01' -> '2030-01-01'
date_fixed AS (
  SELECT
    product_id, name, category_id, brand_id, supplier_id, season, price,
    CASE
      WHEN created_at_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CONCAT_WS('-',
          SUBSTRING(created_at_raw, 1, 4),
          LPAD(COALESCE(NULLIF(SUBSTRING(created_at_raw, 6, 2), '00'), '01'), 2, '0'),
          LPAD(COALESCE(NULLIF(SUBSTRING(created_at_raw, 9, 2), '00'), '01'), 2, '0')
        )
      ELSE NULL
    END AS created_at_fixed_str
  FROM price_clean
),

-- STEP 4: PARSE + FORCE INTO PAST
-- PURPOSE: Convert to DATE and cap future dates at CURRENT_DATE
dates_past AS (
  SELECT
    product_id, name, category_id, brand_id, supplier_id, season, price,
    CASE
      WHEN created_at_fixed_str IS NULL THEN NULL
      WHEN TO_DATE(created_at_fixed_str, 'YYYY-MM-DD') > CURRENT_DATE THEN CURRENT_DATE
      ELSE TO_DATE(created_at_fixed_str, 'YYYY-MM-DD')
    END AS created_at
  FROM date_fixed
)

-- STEP 5: DEDUPLICATE
-- PURPOSE: Guarantee one row per product_id for reliable joins/tests
SELECT DISTINCT ON (product_id)
  product_id,
  name AS product_name,
  category_id,
  brand_id,
  supplier_id,
  price,
  created_at,
  season
FROM dates_past
ORDER BY product_id, created_at DESC NULLS LAST
