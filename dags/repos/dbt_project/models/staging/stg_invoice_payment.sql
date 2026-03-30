{{ config(materialized='view') }}

WITH source AS (
    -- Trỏ trực tiếp vào bảng RAW_INVOICE_PAYMENT_JSON
    SELECT RAW_DATA, INGESTED_AT 
    FROM {{ source('cukcuk', 'invoice_payment_raw') }}
)

SELECT
    -- Các key trong bảng Payment thường là:
    RAW_DATA:ref_id::VARCHAR(50)         AS ref_id,
    RAW_DATA:payment_method::VARCHAR(50) AS payment_method,
    RAW_DATA:amount::FLOAT               AS amount,
    
    -- Đồng bộ ngày báo cáo
    RAW_DATA:report_date::DATE           AS report_date,
    INGESTED_AT
FROM source
WHERE RAW_DATA:ref_id IS NOT NULL