{{ config(
    materialized='incremental',
    alias='view_master_sales_analytics',
    incremental_strategy='merge',
    unique_key=['order_id', 'item_id'],
    cluster_by=['year', 'month']
) }}

WITH header AS (
    SELECT * FROM {{ ref('stg_invoice_header') }}
    {% if is_incremental() %}
        -- Lọc dữ liệu mới để nạp thêm (Incremental)
        WHERE ref_date_at >= DATEADD('day', -{{ var('backfill_days', 3) }}, CURRENT_DATE())
    {% endif %}
),

detail AS (
    SELECT * FROM {{ ref('stg_invoice_detail') }}
    WHERE item_id IS NOT NULL 
),

final AS (
    SELECT
        h.branch_name,
        h.ref_id                                        AS order_id,
        h.ref_no                                        AS invoice_code,
        h.customer_id,
        d.item_name,
        d.item_id,
        h.ref_date_at                                   AS ref_date, 
        h.report_date,
        TO_VARCHAR(h.ref_date_at, 'HH24')               AS report_hour,
        h.day,
        h.month,
        h.year,
        d.quantity,
        (d.amount
         - COALESCE(d.allocation_amount, 0)
         - COALESCE(d.allocation_delivery_promotion_amount, 0)
        )                                               AS net_revenue_pre_tax,
        COALESCE(d.tax_amount, 0)                       AS tax_amount,
        (d.amount
         - COALESCE(d.allocation_amount, 0)
         - COALESCE(d.allocation_delivery_promotion_amount, 0)
         + COALESCE(d.tax_amount, 0)
        )                                               AS net_revenue_inclusive
    FROM detail d
    JOIN header h ON d.ref_id = h.ref_id
    -- 🚨 SỬA TẠI ĐÂY: Lấy các hóa đơn đã thanh toán (thường là status 4)
    -- Nếu muốn lấy tất cả, bạn có thể comment dòng này lại bằng --
    WHERE h.payment_status != 4
)

-- CHỈ DÙNG 1 LỆNH SELECT Ở ĐÂY
SELECT * FROM final
{% if is_incremental() %}
    -- Tránh lỗi so sánh với NULL khi bảng rỗng lần đầu
    AND report_date >= (SELECT COALESCE(MAX(report_date), '2020-01-01') FROM {{ this }})
{% endif %}