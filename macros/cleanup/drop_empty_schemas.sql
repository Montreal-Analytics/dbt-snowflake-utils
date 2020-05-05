{% macro drop_empty_schemas() %}

  {% set cleanup_query %}

      WITH 
      
      ALL_SCHEMAS AS (
        SELECT
          CONCAT_WS('.', CATALOG_NAME, SCHEMA_NAME) AS SCHEMA_NAME
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.SCHEMATA
        WHERE 
          SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
      ),

      NON_EMPTY_SCHEMAS AS (
        SELECT
          DISTINCT CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA) AS SCHEMA_NAME
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.TABLES
        WHERE 
          TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
      ),

      EMPTY_SCHEMAS AS (
        SELECT * FROM ALL_SCHEMAS
        MINUS
        SELECT * FROM NON_EMPTY_SCHEMAS
      )

      SELECT 
        'DROP SCHEMA ' || SCHEMA_NAME || ';' as DROP_COMMANDS
      FROM 
        EMPTY_SCHEMAS

  {% endset %}

    
  {% set drop_commands = run_query(cleanup_query).columns[0].values() %}


  {% if drop_commands %}
    {% for drop_command in drop_commands %}
      {% do log(drop_command, True) %}
      {% do run_query(drop_command) %}
    {% endfor %}
  {% else %}
    {% do log('No schemas to clean.', True) %}
  {% endif %}
  
{% endmacro %}