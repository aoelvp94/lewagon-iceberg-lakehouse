{#
  Schema-evolution guard for downstream models: use the column when it exists,
  otherwise a default. Lets silver/gold run before AND after `loyalty_tier`
  shows up in the source.
#}
{% macro column_or_default(relation, column_name, default_sql, alias=none) %}
    {%- set names = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- if column_name | lower in names -%}
        coalesce({{ column_name }}, {{ default_sql }})
    {%- else -%}
        {{ default_sql }}
    {%- endif %} as {{ alias or column_name }}
{% endmacro %}
