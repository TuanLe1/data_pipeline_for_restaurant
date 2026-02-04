SELECT *
FROM {{ ref('master_sales_analytics') }}
WHERE net_revenue_inclusive < 0