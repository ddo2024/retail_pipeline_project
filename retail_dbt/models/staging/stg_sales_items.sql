{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'sales_items') }}
