{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'customer_feedback') }}
