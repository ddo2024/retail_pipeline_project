-- models/silver/silver_sales_items.sql
{{ config(materialized='table') }}

-- STEP 1: CAST + TRIM
-- PURPOSE: Enforce stable types for joins and math; keep text-safe casts first
WITH base AS (
  SELECT
    item_id::INT           AS item_id,
    transaction_id::INT    AS transaction_id,
    product_id::INT        AS product_id,
    COALESCE(quantity, 0)::INT                    AS quantity,
    COALESCE((unit_price::TEXT)::NUMERIC, 0)::NUMERIC(18,2) AS unit_price,
    COALESCE((discount::TEXT)::NUMERIC, 0)::NUMERIC(18,2)   AS discount,
    COALESCE((tax::TEXT)::NUMERIC, 0)::NUMERIC(18,2)        AS tax
  FROM {{ ref('stg_sales_items') }}
)

-- STEP 2: BASIC ROW QUALITY
-- PURPOSE: Keep only rows with required keys present
, valid AS (
  SELECT *
  FROM base
  WHERE item_id IS NOT NULL
    AND transaction_id IS NOT NULL
    AND product_id IS NOT NULL
)

-- STEP 3: DEDUPLICATE
-- PURPOSE: Guarantee one row per ITEM_ID (latest non-null values win by ordering)
SELECT DISTINCT ON (item_id)
  item_id,
  transaction_id,
  product_id,
  quantity,
  unit_price,
  discount,
  tax
FROM valid
ORDER BY item_id
