select
    model_name,
    make,
    category,
    year,
    display_name,
    is_in_production,
    first_produced,
    model_id_us,
    model_id_br,
    price
from (
    select
        Model as model_name,
        Make as make,
        Category as category,
        Year as year,
        DisplayName as display_name,
        IsInProduction as is_in_production,
        FirstProducedModel as first_produced,
        USModelID as model_id_us,
        BRModelID as model_id_br,
        DisplayPriceTagUSD as price,
        row_number() over (
            partition by USModelID, BRModelID
            order by Year desc
        ) as rn
    from {{ source('raw', 'model_master-list') }}
)
where rn = 1
