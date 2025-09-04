{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'discount_rules') }}
