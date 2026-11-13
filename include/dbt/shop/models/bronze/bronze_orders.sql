-- Latest version of every order_id (raw keeps the full CDC history).
{{ dedup_latest(source('raw', 'orders'), ['order_id']) }}
