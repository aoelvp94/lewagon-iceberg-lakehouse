select
    p.product_id,
    p.product_name,
    p.category,
    p.current_unit_price,
    count(distinct i.order_id) as orders,
    coalesce(sum(i.quantity), 0) as units_sold,
    coalesce(sum(i.line_amount), 0) as revenue
from {{ ref('dim_products') }} as p
left join {{ ref('fct_order_items') }} as i on i.product_id = p.product_id
group by p.product_id, p.product_name, p.category, p.current_unit_price
