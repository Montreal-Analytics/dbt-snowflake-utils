{% macro grant_ownership_schema_cascade(new_owner_role, destination_schema, destination_database=target.database) %}
  
  {% if new_owner_role and destination_schema %}

    {{ (log("Granting ownership on " ~ destination_database ~ "." ~ destination_schema ~ 
    " and its tables and views to " ~ new_owner_role, info=True)) }}
    
    {% call statement('grant_ownership_cascade', fetch_result=True, auto_begin=False) -%}
        GRANT USAGE ON SCHEMA {{ destination_database }}.{{ destination_schema }}
            TO {{ new_owner_role }};
        GRANT OWNERSHIP ON ALL TABLES IN SCHEMA {{ destination_database }}.{{ destination_schema }}
            TO {{ new_owner_role }} REVOKE CURRENT GRANTS;
        GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA {{ destination_database }}.{{ destination_schema }}
            TO {{ new_owner_role }} REVOKE CURRENT GRANTS;
        GRANT OWNERSHIP ON SCHEMA {{ destination_database }}.{{ destination_schema }}
            TO {{ new_owner_role }} REVOKE CURRENT GRANTS;
    {%- endcall %}
    
    {%- set result = load_result('grant_ownership_cascade') -%}
    {{ log(result['data'][0][0], info=True)}}

  {% else %}
    
    {{ exceptions.raise_compiler_error("Invalid arguments. Missing new owner role and/or destination schema") }}

  {% endif %}

{% endmacro %}
