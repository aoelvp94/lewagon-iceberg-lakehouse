-- Singular test: no bronze row may be older than another raw event for the same key.
-- Catches the late-arriving scenario if someone orders by _ingested_at only.
select b.order_id
from {{ ref('bronze_orders') }} as b
join {{ source('raw', 'orders') }} as r on r.order_id = b.order_id
group by b.order_id, b.updated_at
having max(r.updated_at) > b.updated_at
