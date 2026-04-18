-- tests/assert_order_date_not_in_future.sql
-- Ngày đặt hàng không được lớn hơn ngày hiện tại
select *
from {{ ref('stg_order_header') }}
where order_date > today()