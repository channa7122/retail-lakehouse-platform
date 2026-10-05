{{ config(materialized='table') }}

with distinct_dates as (
    select distinct cast(order_timestamp as date) as date_day
    from {{ ref('stg_orders') }}
)

select
    date_day,
    year(date_day) as date_year,
    month(date_day) as date_month,
    day(date_day) as date_day_of_month,
    date_format(date_day, 'EEEE') as day_name,
    date_format(date_day, 'MMMM') as month_name,
    case when date_format(date_day, 'E') in ('Sat', 'Sun') then true else false end as is_weekend
from distinct_dates