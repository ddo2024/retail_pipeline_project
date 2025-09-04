{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'pricing_history') }}
