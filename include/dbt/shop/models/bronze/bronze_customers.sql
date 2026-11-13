-- Latest version of every customer_id (raw keeps the full CDC history).
{{ dedup_latest(source('raw', 'customers'), ['customer_id']) }}
