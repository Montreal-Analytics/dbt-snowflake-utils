{% macro warehouse_size() %}

    {% set initial_wh = var('snowflake_utils:initial_run_warehouse', none) %}
    {% set full_wh = var('snowflake_utils:full_refresh_run_warehouse', none) %}
    {% set inc_wh = var('snowflake_utils:incremental_run_warehouse', none) %}

    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}

    {#-- use alternative warehouse if full-refresh run #}
    {# if relation is not none and flags.FULL_REFRESH and full_wh is not none #}
    {% if flags.FULL_REFRESH and full_wh is not none %}
        {% if execute %}
            {{ dbt_utils.log_info("Full Refresh Run - Using alternative warehouse " ~ full_wh | upper) }}
        {% endif %}
        {% do return(full_wh) %}

    {#-- use alternative warehouse if incremental run #}
    {% elif relation is not none and flags.FULL_REFRESH == False and inc_wh is not none %}
        {% if execute %}
            {{ dbt_utils.log_info("Incremental Run - Using alternative warehouse " ~ inc_wh | upper) }}
        {% endif %}
        {% do return(inc_wh) %}

    {#-- use alternative warehouse if initial run #}
    {% elif relation is none and initial_wh is not none %}
        {% if execute %} 
            {{ dbt_utils.log_info("Initial Run - Using alternative warehouse " ~ initial_wh | upper) }}
        {% endif %}
        {% do return(initial_wh) %}

    {#-- use target warehouse if variable not configured for a condition #}
    {% else %}
        {{ dbt_utils.log_info("Using target warehouse " ~ target.warehouse | upper) }}
        {% do return(target.warehouse) %}

    {% endif %}

{% endmacro %}
