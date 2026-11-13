-- Moves when the "updates" / "late-arriving" scenarios land.
select
    status,
    count(*) as orders,
    sum(order_amount) as revenue
from {{ ref('fct_orders') }}
group by status
