{{ config(materialized='view') }}

WITH source AS (
    SELECT RAW_DATA, INGESTED_AT FROM {{ source('cukcuk', 'invoice_header_raw') }}
),

deduplicated AS (
    SELECT 
        RAW_DATA, 
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY RAW_DATA:ref_id::VARCHAR 
            ORDER BY INGESTED_AT DESC
        ) AS row_num
    FROM source
)

SELECT
    RAW_DATA:ref_id::VARCHAR(50)           AS ref_id,
    RAW_DATA:ref_no::VARCHAR(50)           AS ref_no,
    RAW_DATA:branch_name::VARCHAR(100)     AS branch_name,
    RAW_DATA:payment_status::INTEGER       AS payment_status,
    RAW_DATA:total_amount::FLOAT           AS total_amount,
    RAW_DATA:customer_id::VARCHAR(50)      AS customer_id,

    -- Xử lý Year/Month từ report_date cho chắc chắn
    COALESCE(RAW_DATA:year::INTEGER, YEAR(RAW_DATA:report_date::DATE))   AS year,
    COALESCE(RAW_DATA:month::INTEGER, MONTH(RAW_DATA:report_date::DATE)) AS month,
    RAW_DATA:day::INTEGER                                               AS day,
    
    -- 🚨 SỬA TẠI ĐÂY: Xử lý linh hoạt cả String và Epoch BigInt
    CASE 
        WHEN IS_INTEGER(RAW_DATA:ref_date) 
        THEN TO_TIMESTAMP_NTZ(RAW_DATA:ref_date::BIGINT / 1000000)
        ELSE TRY_TO_TIMESTAMP(RAW_DATA:ref_date::VARCHAR)
    END AS ref_date_at,

    RAW_DATA:report_date::DATE             AS report_date,
    INGESTED_AT
FROM deduplicated
WHERE row_num = 1