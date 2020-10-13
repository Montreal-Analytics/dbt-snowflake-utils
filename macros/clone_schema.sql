{% macro clone_schema(source_schema, destination_schema, source_database=target.database, destination_database=target.database) %}

  {% if not (source_database and source_schema and destination_database and destination_schema) %}
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing source and/or target schema/database") }}
  {% endif %}

  {{
    log("Cloning existing schema " ~ source_database ~ "." ~ source_schema ~
    " into schema " ~ destination_database ~ "." ~ destination_schema, info=True)
  }}

  {% call statement(tables_to_clone, fetch_result=True, auto_begin=True) -%}
    SELECT
      CASE WHEN table_type = 'VIEW' THEN
        'CREATE OR REPLACE VIEW {{ destination_database }}.{{ destination_schema }}."' || table_name || '" AS (SELECT * FROM ' || '{{ source_database }}.{{ source_schema }}."' || table_name || '");'
      ELSE
        'CREATE OR REPLACE' || IFF(is_transient = 'YES', ' TRANSIENT ', ' ') || 'TABLE {{ destination_database }}.{{ destination_schema }}."' || table_name || '" CLONE ' || '{{ source_database }}.{{ source_schema }}."' || table_name || '";'
      END AS stmt
    FROM
      {{ source_database }}.information_schema.tables
    WHERE
      table_schema = UPPER('{{ source_schema }}')
  {%- endcall %}
  {%- set tables_to_clone = load_result(tables_to_clone) -%}

  {% call statement('create_schema', fetch_result=False, auto_begin=True) -%}
    CREATE SCHEMA IF NOT EXISTS {{ destination_database}}.{{ destination_schema }};
  {%- endcall %}

  {% for clone_table in tables_to_clone['data'] %}
    {{ log("Running " ~ clone_table[0], info=True) }}

    {% call statement('clone_table', fetch_result=False, auto_begin=True) -%}
      {{ clone_table[0] }}
    {%- endcall %}
  {% endfor %}

{% endmacro %}
