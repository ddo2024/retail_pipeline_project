{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'tax_rules') }}
