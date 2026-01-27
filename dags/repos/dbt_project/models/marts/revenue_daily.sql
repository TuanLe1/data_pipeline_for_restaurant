{{ config(
    materialized='table',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['year', 'month']
) }}

WITH raw_data AS (
    -- Đọc từ bảng staging đã được trỏ đúng source
    SELECT * FROM {{ ref('stg_invoice_header') }}
)

SELECT
    report_date,
    branch_name,
    year,
    month,
    -- SỬA Ở ĐÂY: Dùng ref_id thay vì invoice_id
    COUNT(DISTINCT ref_id) as total_orders, 
    
    SUM(total_amount) as total_revenue,
    
    -- Tránh chia cho 0
    CASE 
        WHEN COUNT(DISTINCT ref_id) > 0 THEN SUM(total_amount) / COUNT(DISTINCT ref_id)
        ELSE 0 
    END as aov
FROM raw_data
WHERE total_amount > 0
GROUP BY 1, 2, 3, 4