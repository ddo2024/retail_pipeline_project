-- models/silver/silver_customers.sql
{{ config(materialized='table') }}

-- STEP 1: CAST + CLEAN
-- PURPOSE: Enforce types, trim/normalize text, and prep for safe parsing
WITH base AS (
  SELECT
    customer_id::INT                    AS customer_id,
    INITCAP(TRIM(name))                 AS name,
    LOWER(TRIM(email))                  AS email,
    REGEXP_REPLACE(COALESCE(phone::TEXT, ''), '[^0-9]', '', 'g') AS phone_digits,
    loyalty_program_id::INT             AS loyalty_program_id,
    created_at::TEXT                    AS created_at_raw
  FROM {{ ref('stg_customers') }}
)

-- STEP 2: FIX DATE STRING (HANDLE '00' MONTH/DAY)
-- PURPOSE: Repair malformed dates like '2043-00-03' -> '2043-01-03'
, date_fixed AS (
  SELECT
    customer_id, name, email, phone_digits, loyalty_program_id,
    CASE
      WHEN created_at_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CONCAT_WS('-',
          SUBSTRING(created_at_raw, 1, 4),
          LPAD(COALESCE(NULLIF(SUBSTRING(created_at_raw, 6, 2), '00'), '01'), 2, '0'),
          LPAD(COALESCE(NULLIF(SUBSTRING(created_at_raw, 9, 2), '00'), '01'), 2, '0')
        )
      ELSE NULL
    END AS created_at_fixed_str
  FROM base
)

-- STEP 3: PARSE + FORCE INTO PAST
-- PURPOSE: Convert to DATE and cap any future created_at at CURRENT_DATE
, dated AS (
  SELECT
    customer_id, name, email, phone_digits, loyalty_program_id,
    CASE
      WHEN created_at_fixed_str IS NULL THEN NULL
      WHEN TO_DATE(created_at_fixed_str, 'YYYY-MM-DD') > CURRENT_DATE THEN CURRENT_DATE
      ELSE TO_DATE(created_at_fixed_str, 'YYYY-MM-DD')
    END AS created_at
  FROM date_fixed
)

-- STEP 4: FILTER ESSENTIALS
-- PURPOSE: Keep only usable rows (valid ids and date)
, valid AS (
  SELECT *
  FROM dated
  WHERE customer_id IS NOT NULL
    AND loyalty_program_id IS NOT NULL
    AND created_at IS NOT NULL
)

-- STEP 5: DEDUPLICATE
-- PURPOSE: Guarantee one row per customer_id (latest created_at wins)
SELECT DISTINCT ON (customer_id)
  customer_id,
  name AS customer_name,
  email,
  phone_digits,
  loyalty_program_id,
  created_at
FROM valid
ORDER BY customer_id, created_at DESC NULLS LAST
