{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('cukcuk', 'order_detail') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_detail_id
            ORDER BY order_date DESC
        ) as row_num
    FROM source
)

SELECT * FROM deduplicated 
WHERE row_num = 1