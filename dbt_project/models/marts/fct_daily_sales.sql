{{ config(materialized='table') }}

select
    cast(o.order_timestamp as date) as sales_date,
    o.store_id,
    p.category,
    count(distinct o.order_id) as total_orders,
    sum(oi.quantity) as total_units_sold,
    round(sum(oi.total_price), 2) as daily_gross_revenue,
    round(sum(oi.discount_amount), 2) as daily_discount_amount,
    round(sum(oi.total_price) - sum(oi.discount_amount), 2) as daily_net_revenue
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
join {{ ref('stg_products') }} p on oi.product_id = p.product_id
where o.order_status = 'Completed'
group by cast(o.order_timestamp as date), o.store_id, p.category