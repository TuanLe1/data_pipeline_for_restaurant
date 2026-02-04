{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('cukcuk', 'order_header') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_date DESC
        ) as row_num
    FROM source
)

SELECT * FROM deduplicated 
WHERE row_num = 1