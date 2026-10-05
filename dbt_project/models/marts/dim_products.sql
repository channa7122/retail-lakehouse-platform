{{ config(materialized='table') }}

select
    product_id,
    product_name,
    category,
    sub_category,
    unit_cost,
    retail_price,
    round(retail_price - unit_cost, 2) as profit_margin_amount,
    round(((retail_price - unit_cost) / retail_price) * 100, 2) as profit_margin_pct
from {{ ref('stg_products') }}