{{ config(materialized='view') }}

SELECT 
    *,
    -- Tính toán các cột thời gian từ ref_date để các bảng Mart dùng chung
    toDate(ref_date) as report_date,
    toYear(toDateTime(ref_date)) as year,
    toMonth(toDateTime(ref_date)) as month
FROM {{ source('cukcuk', 'invoice_header') }}
WHERE ref_id != '' AND ref_id IS NOT NULL 
  AND report_date > '2010-01-01'