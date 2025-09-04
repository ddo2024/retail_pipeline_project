{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'loyalty_programs') }}
