{{ config(materialized='view') }}

with source as (
    select * from delta.`/opt/spark/data/silver/order_items`
)

select
    cast(order_item_id as string) as order_item_id,
    cast(order_id as string) as order_id,
    cast(product_id as string) as product_id,
    cast(quantity as int) as quantity,
    cast(unit_price as double) as unit_price,
    cast(discount_amount as double) as discount_amount,
    cast(total_price as double) as total_price,
    cast(order_timestamp as timestamp) as order_timestamp,
    cast(_transformed_at as timestamp) as transformed_at
from source