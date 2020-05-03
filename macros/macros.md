{% docs clone_schema %}
This macro clones a schema to a new location.

To use in the command line:
dbt run-operation clone_schema --args '{"source_schema": "source_schema_name", "destination_schema": "destination_schema_name"}'
{% enddocs %}
