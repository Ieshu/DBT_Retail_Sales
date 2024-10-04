-- macros/transform_utils.sql
{% macro title_case(column_name) %}
    initcap({{ column_name }})  -- Using PostgreSQL's initcap() to title case strings
{% endmacro %}

{% macro safe_cast(column_name, data_type) %}
    CASE 
        WHEN {{ column_name }} IS NULL OR {{ column_name }} = '' THEN NULL
        ELSE CAST({{ column_name }} AS {{ data_type }})
    END
{% endmacro %}
