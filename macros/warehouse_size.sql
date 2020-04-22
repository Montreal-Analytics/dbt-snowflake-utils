{% macro warehouse_size() %}

    {% if not execute or model.config.materialized != 'incremental' %}
        {% do return(target.warehouse) %}

    {% else %}

        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {% if relation is not none
           and var('snowflake-utils:initial_run_warehouse') | length > 0 %}
            {% do return(var('snowflake-utils:initial_run_warehouse')) %}

        {% elseif flags.FULL_REFRESH 
           and var('snowflake-utils:full_refresh_run_warehouse') | length > 0 %}
            {% do return(var('snowflake-utils:full_refresh_run_warehouse')) %}

        {% elseif var('snowflake-utils:incremental_run_warehouse') | length > 0 %}
            {% do return(var('snowflake-utils:incremental_run_warehouse')) %}        

        {% else %}
            {% do return(target.warehouse) %}
        {% endif %}
      
    {% endif %}

{% endmacro %}