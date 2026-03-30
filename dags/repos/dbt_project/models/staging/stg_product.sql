{{ config(materialized='view') }}

WITH source AS (
    SELECT 
        RAW_DATA, 
        INGESTED_AT,
        ROW_NUMBER() OVER (PARTITION BY RAW_DATA:product_id::VARCHAR ORDER BY INGESTED_AT DESC) AS row_num
    FROM {{ source('cukcuk', 'product_raw') }}
)

SELECT
    RAW_DATA:product_id::VARCHAR(50)   AS product_id,
    RAW_DATA:product_code::VARCHAR(50) AS product_code,
    RAW_DATA:product_name::VARCHAR(200) AS product_name,
    RAW_DATA:price::FLOAT              AS price,
    -- Master data thường dùng ngày nạp nếu không có report_date
    COALESCE(RAW_DATA:report_date::DATE, INGESTED_AT::DATE) AS report_date,
    INGESTED_AT
FROM source
WHERE row_num = 1