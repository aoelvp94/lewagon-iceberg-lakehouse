-- Bronze = latest version of every product_id (raw keeps the full CDC history).
with ranked as (
    select
        *,
        row_number() over (
            partition by product_id
            order by updated_at desc, _ingested_at desc
        ) as dedup_rank
    from {{ source('raw', 'products') }}
)

select
    product_id,
    name,
    category,
    unit_price,
    updated_at,
    _op = 'D' as is_deleted,
    _batch_id,
    _ingested_at
from ranked
where dedup_rank = 1
