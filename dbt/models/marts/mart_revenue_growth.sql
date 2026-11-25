-- Exercise 3: month-over-month revenue growth with LAG().
with monthly as (
    select
        d.ym,
        sum(f.total_amount) as revenue
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_date') }}   d on f.date_key = d.date_key
    group by d.ym
)
select
    ym,
    revenue,
    lag(revenue) over (order by ym) as prev_revenue,
    revenue - lag(revenue) over (order by ym) as revenue_change,
    round(
        100.0 * (revenue - lag(revenue) over (order by ym))
        / nullif(lag(revenue) over (order by ym), 0),
        2
    ) as growth_pct                      -- NULL for the first month
from monthly
order by ym
