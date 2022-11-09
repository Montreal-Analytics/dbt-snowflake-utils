{% docs clone_schema %}
This macro clones a schema to a new location.

To use in the command line:
dbt run-operation clone_schema --args '{"source_schema": "source_schema_name", "destination_schema": "destination_schema_name"}'

Or:

dbt run-operation clone_schema --args '{"source_schema": "source_schema_name", "destination_schema": "destination_schema_name",
"source_database": "source_database_name",
"destination_schema": "destination_schema_name"}'
{% enddocs %}

{% docs drop_schema %}
This macro drops a schema in the selected database (defaults to target database if no database is selected).

To use in the command line:
dbt run-operation drop_schema --args '{"schema_name": "schema_to_drop", "database": "database_name"}'
{% enddocs %}

{% docs warehouse_size %}
This macro returns an alternative warehouse if conditions are met. It will, in order, check the following conditions for incremental models:

- The relation doesn't exist (initial run) _and_ a warehouse has been configured
- Full refresh run _and_ a warehouse has been configured

Otherwise, it returns the target warehouse configured in the profile.

#### Usage

Call the macro from the `snowflake_warehouse` model configuration:
{% raw %}
```
{{ 
    config(
      snowflake_warehouse=snowflake_utils.warehouse_size()
    )
}}
```
{% endraw %}


#### Macro Configuration

Out-of-the-box, the macro will return the `target.warehouse` for each condition, unless exceptions are configured using one or more of the following [variables](https://docs.getdbt.com/docs/using-variables):

| variable | information | required |
|----------|-------------|:--------:|
|snowflake_utils:initial_run_warehouse|Alternative warehouse when the relation doesn't exist|No|
|snowflake_utils:full_refresh_run_warehouse|Alternative warehouse when doing a `--full-refresh`|No|

An example `dbt_project.yml` configuration:

```yml
# dbt_project.yml

...

models:
    my_project:
        vars:
            'snowflake_utils:initial_run_warehouse': "transforming_xl_wh"
            'snowflake_utils:full_refresh_run_warehouse': "transforming_xl_wh"


```

#### Console Output

When a variable is configured for a conditon _and_ that condition is matched when executing a run, a log message will confirm which condition was matched and which warehouse is being used.

```
12:00:00 | Concurrency: 16 threads (target='dev')
12:00:00 | 
12:00:00 | 1 of 1 START incremental model DBT_MGUINDON.fct_orders... [RUN]
12:00:00 + Initial Run - Using warehouse TRANSFORMING_XL_WH
```
{% enddocs %}

{% docs apply_meta_as_tags %}
This macro applies specific model meta properties as Snowflake tags during on-run-end. This allows you to author Snowflake tags as part of your dbt project.

This macro applies specific model meta properties as Snowflake tags during on-run-end. This allows you to author Snowflake tags as part of your dbt project.

#### Arguments
* `results` (required): The [on-run-end context object](https://docs.getdbt.com/reference/dbt-jinja-functions/on-run-end-context).

#### Usage

First, configure your dbt model to have the 'database_tags' meta property as shown (tag examples borrowed from [here](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies.html)):
{% raw %}
```
schema.yml

models:
  - name: ACCOUNT
    config:
      meta:
        database_tags:
          accounting_row_string: a

    columns:
      - name: ACCOUNT_NAME
        meta:
          database_tags:
            accounting_col_string: b
```
{% endraw %}

The above means:
The Snowflake table ACCOUNT will have the tag 'accounting_row_string' set to the value 'visible'.
Its columns ACCOUNT_NAME and ACCOUNT_NUMBER will both have the tag 'accounting_col_string' set to the value 'visible'

The macro must be called as part of on-run-end, so add the following to dbt_project.yml:
{% raw %}
```
on-run-end: "{{ snowflake_utils.apply_meta_as_tags(results) }}"
```
{% endraw %}

#### Tag removal
This macro only seeks to add or update the tags which are specified in dbt. It won't delete tags which are not defined.
If you need this behaviour, it usually comes naturally as dbt drops and recreates tables/views for most materializations.
If you are using the incremental materialization, be aware of this limitation.

{% enddocs %}