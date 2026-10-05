{{ config(materialized='table') }}

select
    store_id,
    store_name,
    store_city,
    store_state,
    store_region,
    store_type,
    opened_date
from {{ ref('stg_stores') }}