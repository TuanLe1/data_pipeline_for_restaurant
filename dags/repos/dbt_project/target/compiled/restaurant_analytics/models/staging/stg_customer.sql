

SELECT 
    customer_id,
    customer_code, -- Python đã đổi tên rồi, gọi thẳng tên này
    customer_name,
    customer_tel,
    address,
    report_date
FROM "AwsDataCatalog"."restaurant_db"."customer"