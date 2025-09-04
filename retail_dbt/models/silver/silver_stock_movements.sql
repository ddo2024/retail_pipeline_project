{{ config(materialized='table') }}

-- STEP 1: CAST RAW FIELDS
-- PURPOSE: Prevent errors from bad text in numeric/date fields
WITH base AS (
  SELECT
    movement_id::INT          AS movement_id,
    product_id::INT           AS product_id,
    store_id::INT             AS store_id,
    UPPER(TRIM(movement_type)) AS movement_type,
    quantity::TEXT            AS quantity_raw,
    movement_date::TEXT       AS movement_date_raw
  FROM {{ ref('stg_stock_movements') }}
)

-- STEP 2: SAFE QUANTITY CAST
-- PURPOSE: Accept only numeric quantities, filter out 'unknown'
, qty_clean AS (
  SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    CASE WHEN quantity_raw ~ '^-?\d+$' THEN quantity_raw::INT END AS quantity,
    movement_date_raw
  FROM base
)

-- STEP 3: FIX DATE STRING (HANDLE '00' MONTH/DAY)
-- PURPOSE: Repair malformed dates like '2073-00-10' -> '2073-01-10'
, date_fixed AS (
  SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    quantity,
    CASE
      WHEN movement_date_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CONCAT_WS('-',
          SUBSTRING(movement_date_raw, 1, 4),
          LPAD(COALESCE(NULLIF(SUBSTRING(movement_date_raw, 6, 2), '00'), '01'), 2, '0'),
          LPAD(COALESCE(NULLIF(SUBSTRING(movement_date_raw, 9, 2), '00'), '01'), 2, '0')
        )
      ELSE NULL
    END AS movement_date_fixed_str
  FROM qty_clean
)

-- STEP 4: PARSE + FORCE INTO PAST
-- PURPOSE: Cap future dates at CURRENT_DATE
, dated AS (
  SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    quantity,
    CASE
      WHEN movement_date_fixed_str IS NULL THEN NULL
      WHEN TO_DATE(movement_date_fixed_str, 'YYYY-MM-DD') > CURRENT_DATE THEN CURRENT_DATE
      ELSE TO_DATE(movement_date_fixed_str, 'YYYY-MM-DD')
    END AS movement_date
  FROM date_fixed
)

-- STEP 5: ADD SIGNED QUANTITY
-- PURPOSE: Convert IN/OUT to +/- qty, leave TRANSFER as NULL (ambiguous)
, signed AS (
  SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    quantity,
    movement_date,
    CASE
      WHEN movement_type IN ('IN','PURCHASE','TRANSFER_IN','ADJUST_IN') THEN quantity
      WHEN movement_type IN ('OUT','SALE','TRANSFER_OUT','ADJUST_OUT','SHRINK') THEN -quantity
      ELSE NULL
    END AS signed_quantity
  FROM dated
)

-- STEP 6: FILTER ESSENTIALS
-- PURPOSE: Keep only usable rows
, valid AS (
  SELECT *
  FROM signed
  WHERE product_id IS NOT NULL
    AND store_id IS NOT NULL
    AND movement_date IS NOT NULL
    AND quantity IS NOT NULL
    AND quantity <> 0
)

-- STEP 7: DEDUPLICATE
-- PURPOSE: Guarantee one row per movement_id
SELECT DISTINCT ON (movement_id)
  movement_id,
  product_id,
  store_id,
  movement_type,
  quantity,
  movement_date,
  signed_quantity
FROM valid
ORDER BY movement_id, movement_date DESC NULLS LAST
