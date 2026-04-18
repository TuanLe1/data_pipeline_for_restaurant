-- Test FAIL nếu có ngày nào diff > 1%
-- dbt test coi query trả về rows = có lỗi
SELECT
    m.report_date,
    round(sum(m.net_revenue_inclusive), 2)               AS mart_revenue,
    round(sum(d.amount + coalesce(d.tax_amount, 0)), 2)  AS silver_revenue,
    round(abs(sum(m.net_revenue_inclusive)
        - sum(d.amount + coalesce(d.tax_amount, 0)))
        / nullIf(sum(d.amount + coalesce(d.tax_amount, 0)), 0) * 100,
    2)                                                   AS diff_pct
FROM restaurant_db.master_sales_analytics m
JOIN restaurant_db.invoice_detail d
    ON  m.order_id      = d.ref_id
    AND m.item_id       = d.item_id
    AND m.ref_detail_id = d.ref_detail_id
JOIN restaurant_db.invoice_header h
    ON d.ref_id = h.ref_id
WHERE m.report_date >= today() - 7
  AND toDate(d.ref_date) >= today() - 7
GROUP BY m.report_date
HAVING diff_pct > 1   -- trả về rows → test FAIL → Airflow báo đỏ