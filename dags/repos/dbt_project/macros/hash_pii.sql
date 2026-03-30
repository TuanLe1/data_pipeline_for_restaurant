{% macro hash_pii(column_name) %}
    MD5(CAST({{ column_name }} AS VARCHAR))
{% endmacro %}