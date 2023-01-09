{#
-- This macro drops a database.
#}
{% macro drop_database(database_name) %}

  {% if database_name %}

    {{ log("Dropping database " ~ database_name ~ "...", info=True) }}

    {% call statement('drop_database', fetch_result=True, auto_begin=False) -%}
        DROP DATABASE {{ database_name }}
    {%- endcall %}

    {%- set result = load_result('drop_database') -%}
    {{ log(result['data'][0][0], info=True)}}

  {% else %}
    
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing database name") }}

  {% endif %}

{% endmacro %}
