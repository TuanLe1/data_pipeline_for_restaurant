-- report_date không được lớn hơn ngày hiện tại
SELECT *
FROM {{ ref('stg_order_header') }}
WHERE report_date > CURRENT_DATE()
