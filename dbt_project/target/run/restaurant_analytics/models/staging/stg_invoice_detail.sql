create or replace view
    "AwsDataCatalog"."restaurant_db"."stg_invoice_detail"
  as
    

SELECT * FROM "AwsDataCatalog"."restaurant_db"."invoice_detail"
