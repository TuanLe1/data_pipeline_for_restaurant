create or replace view
    "AwsDataCatalog"."restaurant_db"."stg_order_header"
  as
    
SELECT * FROM "AwsDataCatalog"."restaurant_db"."order_header"
