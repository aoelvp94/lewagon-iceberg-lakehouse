-- Order entity (grain: order_id) with amounts rolled up from its lines.
with lines as (
    select
        order_id,
        count(*) as n_lines,
        sum(quantity) as n_units,
        sum(line_amount) as order_amount
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_ts,
    cast(o.order_ts as date) as order_date,
    o.status,
    coalesce(l.n_lines, 0) as n_lines,
    coalesce(l.n_units, 0) as n_units,
    coalesce(l.order_amount, 0) as order_amount,
    o.updated_at
from {{ ref('bronze_orders') }} as o
left join lines as l on l.order_id = o.order_id
where not o.is_deleted
