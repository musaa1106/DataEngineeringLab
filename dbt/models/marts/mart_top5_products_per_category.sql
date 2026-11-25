-- Exercise 2: the 5 best-selling products (by revenue) in each category.
with product_revenue as (
    select
        p.category,
        p.product_id,
        p.product_name,
        sum(f.quantity)     as units_sold,
        sum(f.total_amount) as revenue
    from {{ ref('fact_sales') }}  f
    join {{ ref('dim_product') }} p on f.product_id = p.product_id
    group by p.category, p.product_id, p.product_name
)
select
    category,
    row_number() over (partition by category order by revenue desc, product_id) as rank_in_category,
    product_id,
    product_name,
    units_sold,
    revenue
from product_revenue
qualify rank_in_category <= 5
order by category, rank_in_category
