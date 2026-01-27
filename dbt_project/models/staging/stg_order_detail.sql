{{ config(materialized='view') }}
SELECT * FROM {{ source('cukcuk', 'order_detail') }}