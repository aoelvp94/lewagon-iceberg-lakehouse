-- Bronze = latest version of every order_id (raw keeps the full CDC history).
-- Ordering by updated_at (not by arrival) is what makes late-arriving events harmless.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc, _ingested_at desc
        ) as dedup_rank
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    order_ts,
    status,
    updated_at,
    _op = 'D' as is_deleted,
    _batch_id,
    _ingested_at
from ranked
where dedup_rank = 1
