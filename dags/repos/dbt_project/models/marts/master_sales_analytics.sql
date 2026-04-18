{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ref_date)',
    order_by=['report_date', 'order_id', 'item_id', 'ref_detail_id'],
    unique_key=['report_date', 'order_id', 'item_id', 'ref_detail_id']
) }}

/*
  THAY ĐỔI so với version cũ:
  1. materialized_view  →  incremental
     Lý do: MV với INNER JOIN có race condition — nếu header và detail
     không nằm trong cùng batch INSERT, JOIN miss và order mất khỏi mart.
     incremental để dbt kiểm soát JOIN sau khi cả hai đã có đủ trong Silver.

  2. Thêm unique_key để dbt upsert đúng khi backfill chạy lại.

  3. Thêm incremental filter lookback 2 ngày trên cả header lẫn detail.
     - is_incremental() = false (lần đầu): load toàn bộ history.
     - is_incremental() = true  (các lần sau): chỉ reprocess 2 ngày gần nhất.
*/

WITH header AS (
    SELECT * FROM {{ ref('stg_invoice_header') }}
    {% if is_incremental() %}
    WHERE toDate(ref_date) >= toDate(now()) - 2
    {% endif %}
),

detail AS (
    SELECT * FROM {{ ref('stg_invoice_detail') }}
    {% if is_incremental() %}
    WHERE toDate(ref_date) >= toDate(now()) - 2
    {% endif %}
),

final AS (
    SELECT
        h.branch_name                                AS branch_name,
        h.ref_id                                     AS order_id,
        h.ref_no                                     AS invoice_code,
        h.customer_id,
        d.ref_detail_id,
        d.item_name,
        d.item_id,
        assumeNotNull(toDateTime(h.ref_date))        AS ref_date,
        toDate(h.ref_date)                           AS report_date,
        formatDateTime(toDateTime(h.ref_date), '%H') AS report_hour,
        d.quantity,
        (d.amount - coalesce(d.allocation_amount, 0)) AS net_revenue_pre_tax,
        coalesce(d.tax_amount, 0)                    AS tax_amount,
        (d.amount + coalesce(d.tax_amount, 0))       AS net_revenue_inclusive
    FROM detail d
    INNER JOIN header h ON d.ref_id = h.ref_id
)

SELECT * FROM final