{{ config(materialized='view') }}

SELECT 
    customer_id,
    customer_code,
    
    {{ hash_pii('customer_name') }} as customer_name_hashed, 
    {{ hash_pii('customer_tel') }} as customer_tel_hashed,
    
    address,
    report_date
FROM {{ source('cukcuk', 'customer') }}