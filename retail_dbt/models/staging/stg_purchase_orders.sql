{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'purchase_orders') }}
