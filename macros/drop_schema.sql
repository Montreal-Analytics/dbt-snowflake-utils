{#
-- This macro drops a schema in the selected database (defaults to target
-- database if no database is selected).
#}
{% macro drop_schema(
  schema_name,
  database_name=target.database
) %}

  {% if schema_name %}

    {{ log("Dropping schema " ~ database_name ~ "." ~ schema_name ~ "...", info=True) }}

    {% call statement('drop_schema', fetch_result=True, auto_begin=False) -%}
        DROP SCHEMA {{ database_name }}.{{ schema_name }}
    {%- endcall %}

    {%- set result = load_result('drop_schema') -%}
    {{ log(result['data'][0][0], info=True)}}

  {% else %}
    
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing schema name") }}

  {% endif %}

{% endmacro %}
