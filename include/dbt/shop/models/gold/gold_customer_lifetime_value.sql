select
    c.customer_id,
    c.full_name,
    c.country,
    c.loyalty_tier,
    c.is_deleted,
    count(o.order_id) as orders,
    coalesce(sum(o.order_amount), 0) as lifetime_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('dim_customers') }} as c
left join {{ ref('fct_orders') }} as o on o.customer_id = c.customer_id
group by c.customer_id, c.full_name, c.country, c.loyalty_tier, c.is_deleted
