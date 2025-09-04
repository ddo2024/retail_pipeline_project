{{ config(materialized='table') }}

-- STEP 1: CAST + TRIM
-- PURPOSE: enforce types and tidy brand names
WITH base AS (
  SELECT
    brand_id::INT AS brand_id,
    TRIM(name)    AS name
  FROM {{ ref('stg_brands') }}
)

-- STEP 2: DEDUPLICATE
-- PURPOSE: keep one row per brand_id
SELECT DISTINCT ON (brand_id)
  brand_id,
  name
FROM base
ORDER BY brand_id, name NULLS LAST
