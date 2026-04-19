{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by=['report_date', 'branch_name'],
        post_hook=[
            "ALTER TABLE {{ this }} MODIFY SETTING storage_policy = 'hot_to_cold'",
            "ALTER TABLE {{ this }} MODIFY TTL assumeNotNull(report_date) + INTERVAL 30 DAY TO DISK 'minio_cold'"
        ]
    )
}}

WITH raw_data AS (
    -- Đọc từ bảng staging đã được trỏ đúng source
    SELECT * FROM {{ ref('stg_invoice_header') }}
)

SELECT
    report_date,
    branch_name,
    year,
    month,
    COUNT(DISTINCT ref_id) as total_orders, 
    
    SUM(total_amount) as total_revenue,
    
    CASE 
        WHEN COUNT(DISTINCT ref_id) > 0 THEN SUM(total_amount) / COUNT(DISTINCT ref_id)
        ELSE 0 
    END as aov
FROM raw_data
WHERE total_amount > 0
GROUP BY 1, 2, 3, 4