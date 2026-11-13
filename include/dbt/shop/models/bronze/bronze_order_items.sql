-- Latest version of every (order_id, line_number).
{{ dedup_latest(source('raw', 'order_items'), ['order_id', 'line_number']) }}
