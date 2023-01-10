{#
-- This macro clones the source database into the destination database and
-- optionally grants ownership over it, its schemata, and its schemata's tables
-- and views to a new owner.
#}
{% macro clone_database(
  source_database,
  destination_database,
  new_owner_role=''
) %}
  
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

  {% if new_owner_role != '' %}

    {% set list_schemas_query %}
    -- get all schemata within the cloned database to then iterate through them and
    -- change their ownership
    SELECT schema_name
    FROM {{ destination_database }}.information_schema.schemata
    WHERE schema_name != 'INFORMATION_SCHEMA'
    {% endset %}

    {% set results = run_query(list_schemas_query) %}

    {% if execute %}
        {# Return the first column #}
        {% set schemata_list = results.columns[0].values() %}
    {% else %}
        {% set schemata_list = [] %}
    {% endif %}

    {% for schema_name in schemata_list %}

        {{ grant_ownership_on_schema_objects(new_owner_role, schema_name, destination_database) }}

    {% endfor %}

    {{ log("Grant ownership on " ~ destination_database ~ " to " ~ new_owner_role, info=True)}}

    {% call statement('clone_database', fetch_result=True, auto_begin=False) -%}
        GRANT ALL PRIVILEGES ON DATABASE {{ destination_database }} TO {{ new_owner_role }};
    {%- endcall %}

  {% endif %}

{% endmacro %}
