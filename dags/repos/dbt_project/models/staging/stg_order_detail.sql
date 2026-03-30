{{ config(materialized='view') }}

WITH source AS (
    -- Trỏ trực tiếp vào bảng RAW_ORDER_DETAIL_JSON thông qua identifier trong sources.yml
    SELECT RAW_DATA, INGESTED_AT 
    FROM {{ source('cukcuk', 'order_detail_raw') }}
)

SELECT
    -- Sửa lại các key cho đúng với thực tế trong bảng Detail
    RAW_DATA:order_id::VARCHAR(50)         AS order_id,
    RAW_DATA:order_detail_id::VARCHAR(50)  AS order_detail_id,
    RAW_DATA:item_id::VARCHAR(50)          AS item_id,
    RAW_DATA:item_name::VARCHAR(200)       AS item_name,
    RAW_DATA:quantity::FLOAT               AS quantity,
    RAW_DATA:price::FLOAT                  AS price,
    RAW_DATA:amount::FLOAT                 AS amount,
    
    -- Đồng bộ ngày báo cáo
    RAW_DATA:report_date::DATE             AS report_date,
    INGESTED_AT
FROM source
WHERE RAW_DATA:order_id IS NOT NULL