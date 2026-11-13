-- Bronze = latest version of every customer_id.
-- raw keeps every CDC event (append-only Iceberg). We keep the row with the most
-- recent updated_at; exact re-sends (same updated_at) are broken by the latest ingestion.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _ingested_at desc
        ) as dedup_rank
    from {{ source('raw', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    -- Schema evolution: loyalty_tier only exists once the "schema-evolution" batch lands.
    -- Before that we still expose the column (null) so silver/gold never break.
    {%- set raw_columns = adapter.get_columns_in_relation(source('raw', 'customers')) | map(attribute='name') | map('lower') | list %}
    {% if 'loyalty_tier' in raw_columns %}loyalty_tier{% else %}cast(null as varchar){% endif %} as loyalty_tier,
    created_at,
    updated_at,
    _op = 'D' as is_deleted,
    _batch_id,
    _ingested_at
from ranked
where dedup_rank = 1
