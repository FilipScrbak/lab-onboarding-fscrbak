

select

StoreID as store_id,
StoreName as store_name,
Address as address,
CountryCode as country_code,
PostCode as post_code


from {{source("raw", "stroe_tracking_export_v2")}}