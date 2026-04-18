{% macro hash_pii(column_name) %}
    lower(hex(MD5(coalesce(cast({{ column_name }} as String), ''))))
{% endmacro %}