-- Exercise 1: sales per region per month.
select
    c.region,
    d.ym,
    count(distinct f.order_id) as orders,
    sum(f.quantity)            as units_sold,
    sum(f.total_amount)        as revenue
from {{ ref('fact_sales') }}   f
join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
join {{ ref('dim_date') }}     d on f.date_key    = d.date_key
group by c.region, d.ym
order by d.ym, revenue desc
