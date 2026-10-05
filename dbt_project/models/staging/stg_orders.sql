{{ config(materialized='view') }}

with source as (
    select * from delta.`/opt/spark/data/silver/orders`
)

select
    cast(order_id as string) as order_id,
    cast(customer_id as string) as customer_id,
    cast(store_id as string) as store_id,
    cast(order_timestamp as timestamp) as order_timestamp,
    cast(order_status as string) as order_status,
    cast(payment_method as string) as payment_method,
    cast(order_year as int) as order_year,
    cast(order_month as int) as order_month,
    cast(_transformed_at as timestamp) as transformed_at
from source