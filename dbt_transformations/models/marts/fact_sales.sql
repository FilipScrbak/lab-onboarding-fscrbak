{{
    config(
        materialized = "incremental",
        unique_key = "sale_id"
    )
}}

with

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_stores as (
    select * from {{ ref('dim_stores') }}
),

dim_employees as (
    select * from {{ ref('dim_employees') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

sales as (
    select * from {{ ref('stg_sales_complete') }}
),


final as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['sales.transaction_id',
            'sales.model_id',
            'dim_customers.src_customer_id',
            'dim_products.model_id',
            'dim_stores.src_store_id',
            'dim_employees.src_employee_id',
            'dim_date.date_src'
            ]) }} as sale_id,
        sales.transaction_id as src_sale_id,
        dim_customers.customer_id,
        dim_products.model_id,
        dim_stores.src_store_id as store_id,
        dim_employees.src_employee_id as employee_id,
        DATE(sales.transaction_time) as sale_date,
        sales.card_number_hash as card_number,
        TIME(sales.transaction_time) as sale_ts,
        dim_date.date_src as date_id

        from sales

        left join dim_date
            on dim_date.date_day = DATE(sales.transaction_time)

        left join dim_customers
            on cast(dim_customers.customer_id AS INT64) = sales.customer_id

        left join dim_employees
            on dim_employees.employee_id = sales.employee_id

        left join dim_stores
            on dim_stores.store_id = sales.store_id

            left join dim_products
        on (
            (sales.country = 'US' and sales.model_id = dim_products.model_id_us)
            or
            (sales.country = 'BR' and sales.model_id = dim_products.model_id_br)
        )


{% if is_incremental() %}
where sales.transaction_time >= ( select timestamp_sub(max(transaction_time), interval 1 day)
from {{ this }} ) {% endif %})

select distinct * from final