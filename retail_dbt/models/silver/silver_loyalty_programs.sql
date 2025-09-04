{{ config(materialized='table') }}

-- STEP 1: CAST + CLEAN
-- PURPOSE: Normalize ids, names, and numeric fields
WITH base AS (
  SELECT
    loyalty_program_id::INT            AS loyalty_program_id,
    TRIM(name)                         AS name,
    COALESCE(points_per_dollar, 0)::INT AS points_per_dollar
  FROM {{ ref('stg_loyalty_programs') }}
)

-- STEP 2: DEDUPLICATE
-- PURPOSE: Guarantee one row per loyalty_program_id
SELECT DISTINCT ON (loyalty_program_id)
  loyalty_program_id,
  name AS loyalty_program_name,
  points_per_dollar
FROM base
ORDER BY loyalty_program_id, name NULLS LAST
