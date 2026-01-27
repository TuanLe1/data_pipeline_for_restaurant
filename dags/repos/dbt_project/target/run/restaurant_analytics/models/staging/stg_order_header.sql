create or replace view
    "AwsDataCatalog"."restaurant_db"."stg_order_header"
  as
    

WITH source AS (
    SELECT * FROM "AwsDataCatalog"."restaurant_db"."order_header"
),

deduplicated AS (
    SELECT *,
        -- Đánh số thứ tự dựa trên ID.
        -- Dòng nào có ngày sửa đổi (ModifiedDate) mới nhất sẽ là số 1
        ROW_NUMBER() OVER (
            PARTITION BY Id 
            ORDER BY ModifiedDate DESC, CreatedDate DESC
        ) as row_num
    FROM source
)

-- Chỉ lấy dòng số 1 (Mới nhất)
SELECT * FROM deduplicated 
WHERE row_num = 1
