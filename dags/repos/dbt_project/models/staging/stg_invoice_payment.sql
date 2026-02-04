{{ config(materialized='view') }}
SELECT * FROM {{ source('cukcuk', 'invoice_payment') }}