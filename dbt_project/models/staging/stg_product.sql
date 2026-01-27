{{ config(materialized='view') }}

SELECT 
    product_id,
    product_code, -- Python đã đổi tên rồi, gọi thẳng tên này
    product_name,
    unit_name,
    price,
    category_name,
    inactive as is_active,
    report_date
FROM {{ source('cukcuk', 'product') }}