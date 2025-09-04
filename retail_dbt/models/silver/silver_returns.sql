{{ config(materialized='table') }}

-- STEP 1: CAST + CLEAN
-- PURPOSE: Enforce ids, trim reason, and prep for safe parsing
WITH base AS (
  SELECT
    return_id::INT    AS return_id,
    item_id::INT      AS item_id,
    TRIM(reason)      AS reason,
    return_date::TEXT AS return_date_raw
  FROM {{ ref('stg_returns') }}
)

-- STEP 2: FIX DATE STRING (HANDLE '00' MONTH/DAY)
-- PURPOSE: Repair malformed dates like '2033-00-28' -> '2033-01-28'
, date_fixed AS (
  SELECT
    return_id,
    item_id,
    reason,
    CASE
      WHEN return_date_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CONCAT_WS('-',
          SUBSTRING(return_date_raw, 1, 4),
          LPAD(COALESCE(NULLIF(SUBSTRING(return_date_raw, 6, 2), '00'), '01'), 2, '0'),
          LPAD(COALESCE(NULLIF(SUBSTRING(return_date_raw, 9, 2), '00'), '01'), 2, '0')
        )
      ELSE NULL
    END AS return_date_fixed_str
  FROM base
)

-- STEP 3: PARSE + FORCE INTO PAST
-- PURPOSE: Convert to DATE and cap any future return_date at CURRENT_DATE
, dated AS (
  SELECT
    return_id,
    item_id,
    reason,
    CASE
      WHEN return_date_fixed_str IS NULL THEN NULL
      WHEN TO_DATE(return_date_fixed_str, 'YYYY-MM-DD') > CURRENT_DATE THEN CURRENT_DATE
      ELSE TO_DATE(return_date_fixed_str, 'YYYY-MM-DD')
    END AS return_date
  FROM date_fixed
)

-- STEP 4: FILTER VALID ROWS
-- PURPOSE: Drop rows missing keys or usable dates
, valid AS (
  SELECT *
  FROM dated
  WHERE return_id IS NOT NULL
    AND item_id IS NOT NULL
    AND return_date IS NOT NULL
)

-- STEP 5: DEDUPLICATE
-- PURPOSE: Keep one row per return_id (latest date wins on ties)
SELECT DISTINCT ON (return_id)
  return_id,
  item_id,
  reason,
  return_date
FROM valid
ORDER BY return_id, return_date DESC NULLS LAST
