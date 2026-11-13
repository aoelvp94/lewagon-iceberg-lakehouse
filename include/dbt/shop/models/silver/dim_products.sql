-- Product entity with the current list price.
select
    product_id,
    name as product_name,
    category,
    cast(unit_price as numeric(12, 2)) as current_unit_price,
    updated_at
from {{ ref('bronze_products') }}
where not is_deleted
