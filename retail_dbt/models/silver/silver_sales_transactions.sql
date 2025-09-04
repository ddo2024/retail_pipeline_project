{{ config(materialized='incremental', unique_key='transaction_id') }}

-- STEP 1: CAST raw fields
-- PURPOSE: Get stable types; keep zeros; no "today" checks
WITH base AS (
  SELECT
    transaction_id::int                        AS transaction_id,
    customer_id::int                           AS customer_id,
    store_id::int                              AS store_id,
    employee_id::int                           AS employee_id,
    CASE 
      WHEN transaction_date::date > CURRENT_DATE THEN CURRENT_DATE
      ELSE transaction_date::date
    END                                        AS transaction_date,
    COALESCE(total_amount, 0)::numeric(18,2)   AS total_amount,
    payment_id::int                            AS payment_id
  FROM {{ ref('stg_sales_transactions') }}
)

-- STEP 2: DE-DUPLICATE by natural key
-- PURPOSE: Keep one row per transaction_id (latest date wins if dupes exist)
SELECT DISTINCT ON (transaction_id)
  transaction_id, customer_id, store_id, employee_id,
  transaction_date, total_amount, payment_id
FROM base
ORDER BY transaction_id, transaction_date DESC NULLS LAST
