


with

calendar_dates as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    )
    }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(
            ['date(date_day)']
        )}} as date_src,

        cast(date_day as date) as date_day,
        extract(dayofweek from date_day) as day_of_week_number,
        extract(day from date_day) as day_of_month_number,
        extract(dayofyear from date_day) as day_of_year_number,

        extract(week from date_day) as week_of_year_number,
        extract(month from date_day) as month_of_year_number,
        extract(quarter from date_day) as quarter_of_year_number,
        extract(year from date_day) as year_number,

        format_date('%a', date_day) as weekday_name,
        format_date('%b', date_day) as month_name,

        concat(format_date('%b', date_day), ' ', cast(extract(year from date_day) as string)) as month_year,

    from calendar_dates


)

select * from final
