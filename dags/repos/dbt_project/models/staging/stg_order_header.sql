{{ config(materialized='view') }}

WITH source AS (
    SELECT RAW_DATA, INGESTED_AT FROM {{ source('cukcuk', 'order_header_raw') }}
),

deduplicated AS (
    SELECT 
        RAW_DATA, 
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY RAW_DATA:order_id::VARCHAR 
            ORDER BY INGESTED_AT DESC
        ) AS row_num
    FROM source
)

SELECT
    RAW_DATA:order_id::VARCHAR(50)         AS order_id,
    RAW_DATA:branch_name::VARCHAR(100)     AS branch_name,
    RAW_DATA:brand_id::VARCHAR(50)         AS brand_id,
    RAW_DATA:employee_id::VARCHAR(50)      AS employee_id,
    RAW_DATA:order_status::INTEGER         AS status,
    RAW_DATA:total_amount::FLOAT           AS total_amount,
    
    -- Parse Epoch microseconds sang Timestamp
    TO_TIMESTAMP_NTZ(RAW_DATA:order_date::BIGINT / 1000000) AS order_date_at,
    
    -- Dùng report_date có sẵn trong JSON
    RAW_DATA:report_date::DATE             AS report_date,
    RAW_DATA:year::INTEGER                 AS year,
    RAW_DATA:month::INTEGER                AS month,
    RAW_DATA:day::INTEGER                  AS day,
    
    INGESTED_AT
FROM deduplicated
WHERE row_num = 1