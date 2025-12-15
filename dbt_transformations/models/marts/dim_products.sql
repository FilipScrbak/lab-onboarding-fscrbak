  select

  {{dbt_utils.generate_surrogate_key(
            ['model_id_us',
            'model_id_us',
            'dbt_valid_from']
        )}} as model_id,
  model_name,
  make,
  category,
  year,
  is_in_production,
  first_produced,
  model_id_us,
  model_id_br,
  price



  from {{ ref('snapshot_products') }}

  where dbt_valid_to = '9999-12-31 00:00:00 UTC'

