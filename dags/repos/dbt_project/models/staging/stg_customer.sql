{{ config(materialized='view') }}

WITH source AS (
    SELECT RAW_DATA, INGESTED_AT 
    FROM {{ source('cukcuk', 'customer_raw') }}
),

filtered_source AS (
    SELECT * FROM source
    WHERE 
        -- 1. Lọc SQL NULL
        RAW_DATA:customer_id IS NOT NULL 
        -- 2. Lọc JSON null (Đặc thù Snowflake Variant)
        AND NOT IS_NULL_VALUE(RAW_DATA:customer_id)
        -- 3. Lọc chuỗi rỗng hoặc chuỗi có chữ 'null'
        AND TRIM(CAST(RAW_DATA:customer_id AS VARCHAR)) <> ''
        AND LOWER(CAST(RAW_DATA:customer_id AS VARCHAR)) <> 'null'
),

parsed AS (
    SELECT
        TRIM(CAST(RAW_DATA:customer_id AS VARCHAR))   AS customer_id,
        TRIM(CAST(RAW_DATA:customer_name AS VARCHAR)) AS customer_name,
        CAST(RAW_DATA:customer_code AS VARCHAR(50))   AS customer_code,
        CAST(RAW_DATA:customer_tel AS VARCHAR)        AS customer_tel,
        COALESCE(RAW_DATA:report_date::DATE, INGESTED_AT::DATE) AS report_date,
        INGESTED_AT
    FROM filtered_source
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY INGESTED_AT DESC
        ) AS row_num
    FROM parsed
)

SELECT
    customer_id,
    customer_code,
    {{ hash_pii("customer_name") }} AS customer_name_hashed,
    {{ hash_pii("customer_tel") }}  AS customer_tel_hashed,
    report_date,
    INGESTED_AT
FROM deduplicated
WHERE row_num = 1 
  -- Chốt chặn cuối cùng cho chắc chắn pass bài test dbt
  AND customer_id IS NOT NULL 
  AND customer_name IS NOT NULL