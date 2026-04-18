{{ config(
    materialized='view'
) }}

-- ClickHouse không cần ROW_NUMBER() phức tạp ở tầng staging 
-- vì ReplacingMergeTree sẽ tự xử lý ở tầng table.
SELECT * FROM {{ source('cukcuk', 'order_header') }}