{{ config(materialized='view') }}

with source as (
    select * from delta.`/opt/spark/data/silver/customers`
)

select
    cast(customer_id as string) as customer_id,
    cast(first_name as string) as first_name,
    cast(last_name as string) as last_name,
    cast(email as string) as email,
    cast(city as string) as customer_city,
    cast(state as string) as customer_state,
    cast(loyalty_tier as string) as loyalty_tier,
    cast(created_at as date) as customer_since,
    cast(_transformed_at as timestamp) as transformed_at
from source
