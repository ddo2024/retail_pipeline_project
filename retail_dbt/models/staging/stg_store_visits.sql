{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'store_visits') }}
