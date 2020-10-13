{% macro clone_database(source_database, destination_database, exclude_schemas=[]) %}
  -- Assumes the target database already exists

  {% if not (source_database and destination_database) %}
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing source and/or target database") }}
  {% endif %}

  {% call statement('schemas_to_clone', fetch_result=True, auto_begin=True) -%}
    SELECT
      schema_name
    FROM
      {{ source_database }}.information_schema.schemata
    WHERE
      schema_name NOT IN (
        {% for exclude_schema in exclude_schemas %}
          UPPER('{{ exclude_schema }}'),
        {% endfor %}
        'INFORMATION_SCHEMA'
      )
    ;
  {%- endcall %}
  {%- set schemas_to_clone = load_result('schemas_to_clone') -%}

  {% for schema_to_clone in schemas_to_clone['data'] %}
    {{ log("Cloning schema " ~ source_database ~ "." ~ schema_to_clone[0] ~ " to database " ~ destination_database, info=True) }}
    {{ clone_schema(
         source_database=source_database,
         source_schema=schema_to_clone[0],
         destination_database=destination_database,
         destination_schema=schema_to_clone[0]
       )
    }}
  {% endfor %}

{% endmacro %}
