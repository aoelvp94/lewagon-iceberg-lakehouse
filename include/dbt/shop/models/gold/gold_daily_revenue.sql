select
    order_date,
    count(*) as orders,
    sum(n_units) as units,
    sum(order_amount) as revenue,
    cast(avg(order_amount) as numeric(14, 2)) as avg_order_value
from {{ ref('fct_orders') }}
group by order_date
