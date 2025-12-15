  select
   employee_id,
   {{dbt_utils.generate_surrogate_key(
        ['employee_id',
        'dbt_valid_from']
    )}} as src_employee_id,
    first_name,
    last_name,
    phone_number,
    job_position as position,
    hired_on,
    employee_address as  address,
    country_code



  from {{ ref('snapshot_employees') }}

  where dbt_valid_to = '9999-12-31 00:00:00 UTC'