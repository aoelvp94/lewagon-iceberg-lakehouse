-- Shows the schema-evolution effect: everything is 'unknown' until loyalty_tier arrives.
select
    c.country,
    c.loyalty_tier,
    count(distinct c.customer_id) as customers,
    count(o.order_id) as orders,
    coalesce(sum(o.order_amount), 0) as revenue
from {{ ref('dim_customers') }} as c
left join {{ ref('fct_orders') }} as o on o.customer_id = c.customer_id
group by c.country, c.loyalty_tier
