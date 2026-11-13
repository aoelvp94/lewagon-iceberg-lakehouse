-- Order line entity (grain: order_id + line_number). Price is the one paid at order time.
select
    i.order_id,
    i.line_number,
    i.product_id,
    cast(i.quantity as bigint) as quantity,
    cast(i.unit_price as numeric(12, 2)) as unit_price,
    cast(i.quantity * i.unit_price as numeric(14, 2)) as line_amount
from {{ ref('bronze_order_items') }} as i
inner join {{ ref('bronze_orders') }} as o
    on o.order_id = i.order_id and not o.is_deleted
where not i.is_deleted
