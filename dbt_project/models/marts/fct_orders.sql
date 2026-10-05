{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

items_summary as (
    select
        order_id,
        count(order_item_id) as total_items_count,
        sum(quantity) as total_quantity,
        sum(total_price) as gross_revenue,
        sum(discount_amount) as total_discount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.store_id,
    cast(o.order_timestamp as date) as order_date,
    o.order_timestamp,
    o.order_status,
    o.payment_method,
    coalesce(i.total_items_count, 0) as total_items_count,
    coalesce(i.total_quantity, 0) as total_quantity,
    coalesce(round(i.gross_revenue, 2), 0.0) as gross_revenue,
    coalesce(round(i.total_discount, 2), 0.0) as total_discount,
    coalesce(round(i.gross_revenue - i.total_discount, 2), 0.0) as net_revenue
from orders o
left join items_summary i on o.order_id = i.order_id