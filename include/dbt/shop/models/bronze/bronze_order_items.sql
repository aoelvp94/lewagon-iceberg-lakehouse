-- Bronze = latest version of every order line (order_id, line_number).
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id, line_number
            order by updated_at desc, _ingested_at desc
        ) as dedup_rank
    from {{ source('raw', 'order_items') }}
)

select
    order_id,
    line_number,
    product_id,
    quantity,  -- int in the first batches, widened to long by the schema-evolution batch
    unit_price,
    updated_at,
    _op = 'D' as is_deleted,
    _batch_id,
    _ingested_at
from ranked
where dedup_rank = 1
