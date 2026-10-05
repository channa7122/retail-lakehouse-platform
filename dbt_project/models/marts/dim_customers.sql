{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    email,
    customer_city,
    customer_state,
    loyalty_tier,
    customer_since
from {{ ref('stg_customers') }}