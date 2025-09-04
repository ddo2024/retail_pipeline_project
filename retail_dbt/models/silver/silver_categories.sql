{{ config(materialized='table') }}

-- STEP 1: CAST + TRIM
-- PURPOSE: enforce types and tidy names
WITH base AS (
  SELECT
    category_id::INT AS category_id,
    TRIM(name)       AS name
  FROM {{ ref('stg_categories') }}
)

-- STEP 2: DEDUPLICATE
-- PURPOSE: keep one row per category_id
SELECT DISTINCT ON (category_id)
  category_id,
  name AS category_name
FROM base
ORDER BY category_id, name NULLS LAST
