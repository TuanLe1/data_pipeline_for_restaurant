-- Tìm những dòng có doanh thu bị âm (dữ liệu sai)
SELECT *
-- SỬA Ở ĐÂY: Trỏ vào stg_invoice_header (hoặc detail) thay vì stg_orders
FROM {{ ref('stg_invoice_header') }} 
WHERE total_amount < 0