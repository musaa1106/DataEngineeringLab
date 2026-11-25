-- Exercise 4: customers ranked by total spending.
select
    rank() over (order by sum(f.total_amount) desc) as spending_rank,
    c.customer_id,
    c.customer_name,
    c.region,
    count(distinct f.order_id) as orders,
    sum(f.total_amount)        as total_spent,
    round(sum(f.total_amount) / count(distinct f.order_id), 2) as avg_order_value
from {{ ref('fact_sales') }}   f
join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
group by c.customer_id, c.customer_name, c.region
order by spending_rank
