{{ config(materialized='table') }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_invoice_header') }}
),

detail AS (
    SELECT * FROM {{ ref('stg_invoice_detail') }}
),

joined AS (
    SELECT
        s.report_date,
        s.branch_name,
        -- Đảm bảo lấy đúng cột từ bảng Staging Header
        s.year, 
        s.month,
        s.ref_id,
        (d.amount 
         - COALESCE(d.allocation_amount, 0) 
         - COALESCE(d.allocation_delivery_promotion_amount, 0)
         + COALESCE(d.tax_amount, 0)
        ) AS order_revenue
    FROM sales s
    JOIN detail d ON s.ref_id = d.ref_id
    -- Logic: Loại bỏ đơn hủy (4), giữ lại các đơn trạng thái khác (1, 2, 3)
    WHERE s.payment_status != 4
)

SELECT
    report_date,
    branch_name,
    year,
    month,
    COUNT(DISTINCT ref_id)          AS total_orders,
    SUM(order_revenue)              AS total_revenue,
    CASE
        WHEN COUNT(DISTINCT ref_id) > 0
        THEN SUM(order_revenue) / COUNT(DISTINCT ref_id)
        ELSE 0
    END                             AS aov
FROM joined
-- Chỉ lấy những đơn có doanh thu thực tế
WHERE order_revenue > 0
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2