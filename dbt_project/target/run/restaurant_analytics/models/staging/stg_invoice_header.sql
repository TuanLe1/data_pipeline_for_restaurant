create or replace view
    "AwsDataCatalog"."restaurant_db"."stg_invoice_header"
  as
    

SELECT * FROM "AwsDataCatalog"."restaurant_db"."invoice_header"
