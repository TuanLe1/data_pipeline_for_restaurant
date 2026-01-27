create or replace view
    "AwsDataCatalog"."restaurant_db"."stg_order_detail"
  as
    
SELECT * FROM "AwsDataCatalog"."restaurant_db"."order_detail"
