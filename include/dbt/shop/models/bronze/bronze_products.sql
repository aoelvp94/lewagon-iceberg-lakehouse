-- Latest version of every product_id (raw keeps the full CDC history).
{{ dedup_latest(source('raw', 'products'), ['product_id']) }}
