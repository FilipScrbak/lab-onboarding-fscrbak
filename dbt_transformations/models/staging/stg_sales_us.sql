
with us_sales as (
select * from {{source("raw", "us_sales_src")}}
)

select
 CAST(transaction_id AS INT64) as transaction_id,
 transaction_time,
 store as store_id,
 CAST(registered_customer.registered_customer_id AS  INT64) as customer_id,
 mp.model as model_id,
 CAST(employee AS INT64) as employee_id,
 mp.line_num as quantity,
 to_hex(sha256(cast(payment.card_information.card_number as string))) as card_number_hash,
 payment.total.payment as total_amount



    FROM us_sales
    CROSS JOIN UNNEST(us_sales.models_purchased) AS mp