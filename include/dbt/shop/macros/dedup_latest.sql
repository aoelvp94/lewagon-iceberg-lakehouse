{#
  Bronze = "latest version of each business key".

  raw keeps every CDC event (append-only Iceberg). We keep the row with the most
  recent `updated_at`; ties (exact re-sends) are broken by the latest ingestion.
  Columns are read from the relation at run time, so a column added upstream
  (schema evolution) flows into bronze without touching this model.
#}
{% macro dedup_latest(relation, key_columns, order_by=['updated_at desc', '_ingested_at desc']) %}
    {%- if not execute -%}{{ return('select 1') }}{%- endif -%}
    {%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}
    select
        {{ columns | join(',\n        ') }},
        _op = 'D' as is_deleted
    from (
        select
            *,
            row_number() over (
                partition by {{ key_columns | join(', ') }}
                order by {{ order_by | join(', ') }}
            ) as _dedup_rank
        from {{ relation }}
    ) ranked
    where _dedup_rank = 1
{% endmacro %}
