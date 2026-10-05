{{ config(materialized='view') }}

with source as (
    select * from delta.`/opt/spark/data/silver/stores`
)

select
    cast(store_id as string) as store_id,
    cast(store_name as string) as store_name,
    cast(city as string) as store_city,
    cast(state as string) as store_state,
    cast(region as string) as store_region,
    cast(store_type as string) as store_type,
    cast(opened_date as date) as opened_date,
    cast(_transformed_at as timestamp) as transformed_at
from source