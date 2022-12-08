{% macro clone_database(source_database, destination_database) %}
  
  {% if source_database and destination_database %}

    {{ (log("Cloning existing database " ~ source_database ~ 
    " into database " ~ destination_database, info=True)) }}
    
    {% call statement('clone_database', fetch_result=True, auto_begin=False) -%}
        CREATE OR REPLACE DATABASE {{ destination_database }}
          CLONE {{ source_database }};
    {%- endcall %}
    
    {%- set result = load_result('clone_database') -%}
    {{ log(result['data'][0][0], info=True)}}

  {% else %}
    
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing source database and/or destination database") }}

  {% endif %}

{% endmacro %}


{% macro clone_database_with_new_owner(
  new_owner_role,
  source_database,
  destination_database
) %}

{{ clone_database(source_database, destination_database) }}

{% set list_schemas_query %}
select schema_name
from {{ destination_database }}.information_schema.schemata
where schema_name != 'INFORMATION_SCHEMA'
{% endset %}

{% set results = run_query(list_schemas_query) %}

{% if execute %}
    {# Return the first column #}
    {% set schemata_list = results.columns[0].values() %}
{% else %}
    {% set schemata_list = [] %}
{% endif %}

{% for schema_name in schemata_list %}

    {{ grant_ownership_schema_cascade(new_owner_role, schema_name, destination_database) }}

{% endfor %}

{{ log("Grant ownership on " ~ destination_database ~ " to " ~ new_owner_role, info=True)}}

{% call statement('clone_database', fetch_result=True, auto_begin=False) -%}
    GRANT OWNERSHIP ON DATABASE {{ destination_database }} TO {{ new_owner_role }};
{%- endcall %}

{% endmacro %}
