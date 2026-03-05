{% macro trimmer(column_name) %}
    {{ column_name | replace(' ', '_') | trim | upper}}
{% endmacro %}