with src as (

    select *
    from {{ source('mart_src', 'us_sales_src') }}

),

flattened as (

    select
        registered_customer.registered_customer_id   as customer_id,
        registered_customer.first_name               as first_name,
        registered_customer.last_name                as last_name,
        registered_customer.gender                   as gender,
        registered_customer.registered_since         as registered_since,
        registered_customer.referred_by              as referred_by,
        registered_customer.address.country_code     as address_country,
        registered_customer.address.city             as address_city,
        registered_customer.address.address          as address_line,
        registered_customer.contact.phone_number     as phone_number,
        transaction_time,

        ROW_NUMBER() OVER (
            PARTITION BY registered_customer.registered_customer_id
            ORDER BY transaction_time DESC
        ) AS rn

    from src
    where registered_customer.registered_customer_id is not null
)

select
    customer_id,
    first_name,
    last_name,
    gender,
    registered_since,
    referred_by,
    address_country,
    address_city,
    address_line,
    phone_number,
    transaction_time
from flattened
where rn = 1
