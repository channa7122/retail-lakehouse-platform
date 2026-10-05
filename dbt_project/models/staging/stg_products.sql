{{ config(materialized='view') }}

with source as (
    select * from delta.`/opt/spark/data/silver/products`
)

select
    cast(product_id as string) as product_id,
    cast(product_name as string) as product_name,
    cast(category as string) as category,
    cast(sub_category as string) as sub_category,
    cast(unit_cost as double) as unit_cost,
    cast(retail_price as double) as retail_price,
    cast(_transformed_at as timestamp) as transformed_at
from source
