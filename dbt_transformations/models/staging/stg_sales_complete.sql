select * from  {{ ref('stg_sales_us') }}
UNION ALL
select * from {{ ref('stg_sales_br') }}