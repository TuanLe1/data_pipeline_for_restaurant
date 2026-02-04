{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['year', 'month'],
    alias='view_master_sales_analytics',
    incremental_strategy='merge', 
    unique_key=['order_id', 'item_id']
) }}

WITH header AS (
    SELECT * FROM {{ ref('stg_invoice_header') }}
    {% if is_incremental() %}
        WHERE ref_date >= date_add('day', -{{ var('backfill_days', 3) }}, current_date)
    {% endif %}
),

detail AS (
    SELECT * FROM {{ ref('stg_invoice_detail') }}
),

final AS (
    SELECT
        h.branch_name,
        h.ref_id AS order_id,
        h.ref_no AS invoice_code,
        h.customer_id,
        d.item_name,
        d.item_id,
        
        h.ref_date,
        CAST(h.ref_date AS DATE) AS report_date,
        date_format(h.ref_date, '%H') AS report_hour,
        h.day, 
        h.month, 
        h.year,
        d.quantity,
        
        -- [A] Doanh thu thuần
        (d.amount 
         - COALESCE(d.allocation_amount, 0) 
         - COALESCE(d.allocation_delivery_promotion_amount, 0)
        ) AS net_revenue_pre_tax,

        -- [B] Thuế VAT
        COALESCE(d.tax_amount, 0) AS tax_amount,

        -- [C] Doanh thu tổng
        (d.amount 
         - COALESCE(d.allocation_amount, 0) 
         - COALESCE(d.allocation_delivery_promotion_amount, 0)
         + COALESCE(d.tax_amount, 0)
        ) AS net_revenue_inclusive

    FROM detail d
    JOIN header h 
        ON d.ref_id = h.ref_id
    WHERE h.payment_status <> 4
)

SELECT * FROM final

{% if is_incremental() %}
    WHERE report_date >= (SELECT max(report_date) FROM {{ this }})
{% endif %}