-- Customer entity. Deleted customers stay (orders still point to them) but PII is masked.
with c as (
    select * from {{ ref('bronze_customers') }}
)

select
    customer_id,
    case when is_deleted then null else first_name || ' ' || last_name end as full_name,
    case when is_deleted then null else email end as email,
    case when is_deleted then null else split_part(email, '@', 2) end as email_domain,
    country,
    {{ column_or_default(ref('bronze_customers'), 'loyalty_tier', "'unknown'") }},
    is_deleted,
    created_at,
    updated_at
from c
