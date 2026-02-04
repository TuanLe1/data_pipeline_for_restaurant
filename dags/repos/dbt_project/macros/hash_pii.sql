{% macro hash_pii(column_name) %}
    to_hex(md5(to_utf8(cast({{ column_name }} as varchar))))
{% endmacro %}