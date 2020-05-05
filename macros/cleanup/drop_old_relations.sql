{% macro drop_old_relations(age_cutoff_in_hours) %}

  {% set cleanup_query %}

      WITH 
      
      MODELS_TO_DROP AS (
        SELECT
          CASE 
            WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
            WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
          END AS RELATION_TYPE,
          CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) AS RELATION_NAME
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.TABLES
        WHERE 
          TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          AND 
          LAST_ALTERED < DATEADD('HOUR', -{{ age_cutoff_in_hours }}, CURRENT_TIMESTAMP)
      )

      SELECT 
        'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
      FROM 
        MODELS_TO_DROP

  {% endset %}
    
  {% set drop_commands = run_query(cleanup_query).columns[0].values() %}

  {% if drop_commands %}
    {% for drop_command in drop_commands %}
      {% do log(drop_command, True) %}
      {% do run_query(drop_command) %}
    {% endfor %}
  {% else %}
    {% do log('No relations to clean.', True) %}
  {% endif %}
  
{% endmacro %}