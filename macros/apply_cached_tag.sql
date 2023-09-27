{% macro apply_cached_tag(this,tag_name,default_value) %}
    {%- set tag_database = this.database -%}
    {%- set tag_schema = this.schema -%}
    {%- set cached_tag_value = get_cached_tag_value(tag_database,tag_schema,this,tag_name,default_value) -%}


    {%- call statement('apply_cached_tag', fetch_result=True) -%}
        ALTER TABLE {{ this }} SET TAG {{ tag_database }}.{{ tag_schema }}.{{ tag_name }} = '{{ cached_tag_value }}'
    {%- endcall -%}
{% endmacro %}


{% macro get_cached_tag_value(database,schema,this,tag_name,default_value) %}
  {% set get_cached_tag_value_query %}
  SELECT coalesce((SELECT tag_value FROM {{tag_database}}.staging.stg_account_usage__tag_references WHERE lower(concat_ws('.',object_database,object_schema,object_name)) = '{{ this | lower }}' AND lower(tag_schema) = '{{ tag_schema | lower}}' AND lower(tag_name) = '{{tag_name|lower}}' AND object_deleted is null),'{{default_value}}')
  {% endset %}
  {% set results = run_query(get_cached_tag_value_query) %}
  {% if execute %}
  {# Return the first column #}
  {% set result_scalar = results.columns[0].values()[0] %}
  {% else %}
  {% set result_scalar = "" %}
  {% endif %}
  {{ return(result_scalar) }}

{% endmacro %}
