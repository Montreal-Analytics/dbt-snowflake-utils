{% macro clone_schema(source_schema, destination_schema, source_database=target.database, destination_database=target.database) %}
  
  {% if source_schema and destination_schema %}

    {{ (log("Cloning existing schema " ~ source_database ~ "." ~ source_schema ~ 
    " into schema " ~ destination_database ~ "." ~ destination_schema, info=True)) }}
    
    {% call statement('clone_schema', fetch_result=True, auto_begin=False) -%}
        CREATE OR REPLACE SCHEMA {{ destination_database }}.{{ destination_schema }} 
          CLONE {{ source_database }}.{{ source_schema }}
    {%- endcall %}
    
    {%- set result = load_result('clone_schema') -%}
    {{ log(result['data'][0][0], info=True)}}

  {% else %}
    
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing source schema and/or destination schema") }}

  {% endif %}

{% endmacro %}
