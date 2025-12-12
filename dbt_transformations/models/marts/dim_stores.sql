select
   {{dbt_utils.generate_surrogate_key(
            ['store_id',
            'dbt_valid_from']
        )}} as src_store_id,
  store_id,
  store_name,
  address,
  country_code,
  post_code


  from {{ ref('snapshots_stores') }}

  where dbt_valid_to = '9999-12-31 00:00:00 UTC'

