{% macro create_udfs() %}

create schema if not exists {{target.schema}};

{{create_udf_business_days()}};

{% endmacro %}
