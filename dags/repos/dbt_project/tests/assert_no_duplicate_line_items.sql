-- Test FAIL nếu mart_rows != mart_unique_keys
SELECT
    report_date,
    count(*)                                              AS mart_rows,
    countDistinct(concat(order_id, item_id, ref_detail_id)) AS unique_keys
FROM restaurant_db.master_sales_analytics
WHERE report_date >= today() - 7
GROUP BY report_date
HAVING mart_rows != unique_keys  -- trả về rows → test FAIL