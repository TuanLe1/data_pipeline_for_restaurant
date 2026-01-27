{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('cukcuk', 'order_detail') }}
),

deduplicated AS (
    SELECT *,
        -- Chi tiết đơn hàng cũng cần lọc theo ID của chính nó (OrderDetailId)
        -- Hoặc nếu không có ID chi tiết, phải lọc theo OrderId + ProductId
        ROW_NUMBER() OVER (
            PARTITION BY Id  -- Giả sử đây là ID riêng của từng dòng chi tiết
            ORDER BY ModifiedDate DESC, CreatedDate DESC
        ) as row_num
    FROM source
)

SELECT * FROM deduplicated 
WHERE row_num = 1