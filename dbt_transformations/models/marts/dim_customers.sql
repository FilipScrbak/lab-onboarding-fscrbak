 select
    {{dbt_utils.generate_surrogate_key(
            ['customer_id',
            'dbt_valid_from']
        )}} as src_customer_id,
    customer_id,
    first_name,
    last_name,
    address_country as country ,
    address_line as address,
    registered_since as signup_date,
    referred_by

  from {{ ref('snapshot_customers') }}

  where dbt_valid_to = '9999-12-31 00:00:00 UTC'
