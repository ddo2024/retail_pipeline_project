-- models/silver/silver_stores.sql
{{ config(materialized='table') }}

-- STEP 1: CAST + TIDY
-- PURPOSE: Enforce stable types and trimmed text for reliable joins and filters
WITH base AS (
  SELECT
    store_id::INT     AS store_id,
    TRIM(name)        AS name,
    TRIM(location)    AS location,
    manager_id::INT   AS manager_id
  FROM {{ ref('stg_stores') }}
)

-- STEP 2: DEDUPLICATE
-- PURPOSE: Keep exactly one row per store_id for uniqueness tests and downstream joins
SELECT DISTINCT ON (store_id)
  store_id, name AS store_name, location, manager_id
FROM base
ORDER BY store_id, name NULLS LAST
 