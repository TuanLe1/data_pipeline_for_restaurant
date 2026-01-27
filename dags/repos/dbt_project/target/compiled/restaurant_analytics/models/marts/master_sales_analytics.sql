

-- alias='view_master_sales_analytics' giúp dbt tạo ra bảng có tên Y HỆT view cũ của bạn.
-- Looker Studio sẽ tự động nhận diện bảng này mà không cần config lại.

WITH header AS (
    SELECT * FROM "AwsDataCatalog"."restaurant_db"."stg_invoice_header"
),

detail AS (
    SELECT * FROM "AwsDataCatalog"."restaurant_db"."stg_invoice_detail"
)

SELECT 
    -- 1. Dimensions
    h.branch_name,
    h.ref_id AS order_id,
    h.ref_no AS invoice_code,
    h.customer_id,
    d.item_name,
    d.item_id,
    
    -- 2. Time
    h.ref_date,
    -- Ép kiểu Date an toàn hơn cho Athena Iceberg
    CAST(h.ref_date AS DATE) AS report_date,
    date_format(h.ref_date, '%H') AS report_hour,
    h.day, 
    h.month, 
    h.year,

    -- 3. Metrics
    d.quantity,
    
    -- [A] Doanh thu thuần (Pre-tax Revenue)
    (d.amount 
     - COALESCE(d.allocation_amount, 0) 
     - COALESCE(d.allocation_delivery_promotion_amount, 0)
    ) AS net_revenue_pre_tax,

    -- [B] Thuế VAT
    COALESCE(d.tax_amount, 0) AS tax_amount,

    -- [C] Doanh thu tổng (Gross Revenue)
    (d.amount 
     - COALESCE(d.allocation_amount, 0) 
     - COALESCE(d.allocation_delivery_promotion_amount, 0)
     + COALESCE(d.tax_amount, 0)
    ) AS net_revenue_inclusive

FROM detail d
JOIN header h 
    ON d.ref_id = h.ref_id
WHERE h.payment_status <> 4