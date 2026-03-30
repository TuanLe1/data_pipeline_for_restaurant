{{ config(materialized='view') }}

WITH source AS (
    SELECT RAW_DATA, INGESTED_AT 
    FROM {{ source('cukcuk', 'invoice_detail_raw') }}
)

SELECT
    RAW_DATA:ref_id::VARCHAR(50)           AS ref_id,
    RAW_DATA:ref_detail_id::VARCHAR(50)    AS ref_detail_id,
    RAW_DATA:item_id::VARCHAR(50)          AS item_id,
    RAW_DATA:item_name::VARCHAR(200)       AS item_name,
    RAW_DATA:quantity::FLOAT               AS quantity,
    RAW_DATA:amount::FLOAT                 AS amount,
    COALESCE(RAW_DATA:tax_amount::FLOAT, 0) AS tax_amount,
    COALESCE(RAW_DATA:allocation_amount::FLOAT, 0) AS allocation_amount,
    
    -- 🚨 THÊM CỘT NÀY ĐỂ HẾT LỖI MASTER_SALES_ANALYTICS
    COALESCE(RAW_DATA:allocation_delivery_promotion_amount::FLOAT, 0) AS allocation_delivery_promotion_amount,
    
    RAW_DATA:report_date::DATE             AS report_date,
    INGESTED_AT
FROM source
WHERE RAW_DATA:ref_id IS NOT NULL