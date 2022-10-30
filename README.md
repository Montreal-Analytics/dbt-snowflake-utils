# Snowflake Utils

This [dbt](https://github.com/dbt-labs/dbt-core) package contains Snowflake-specific macros that can be (re)used across dbt projects.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/montreal-analytics/snowflake_utils/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Prerequisites
Snowflake Utils is compatible with dbt 0.20.0 and later.

----

## Macros

### snowflake_utils.warehouse_size() ([source](macros/warehouse_size.sql))
This macro returns an alternative warehouse if conditions are met. It will, in order, check the following conditions for incremental models:

- Full refresh run _and_ a warehouse has been configured
- Incremental run _and_ a warehouse has been configured
- The relation doesn't exist (initial run) _and_ a warehouse has been configured

Otherwise, it returns the target warehouse configured in the profile.

#### Usage

Call the macro from the `snowflake_warehouse` model configuration:
```
{{ 
    config(
      snowflake_warehouse=snowflake_utils.warehouse_size()
    )
}}
```

#### Macro Configuration

Out-of-the-box, the macro will return the `target.warehouse` for each condition, unless exceptions are configured using one or more of the following [variables](https://docs.getdbt.com/docs/using-variables):

| variable | information | required |
|----------|-------------|:--------:|
|snowflake_utils:initial_run_warehouse|Alternative warehouse when the relation doesn't exist|No|
|snowflake_utils:full_refresh_run_warehouse|Alternative warehouse when doing a `--full-refresh`|No|
|snowflake_utils:incremental_run_warehouse|Default warehouse for incremental runs|No|

An example `dbt_project.yml` configuration:

```yml
# dbt_project.yml

...
vars:
    'snowflake_utils:initial_run_warehouse': "transforming_xl_wh"
    'snowflake_utils:full_refresh_run_warehouse': "transforming_xl_wh"
    'snowflake_utils:incremental_run_warehouse': "transforming_m_wh"
```

#### Console Output

When a variable is configured for a conditon _and_ that condition is matched when executing a run, a log message will confirm which condition was matched and which warehouse is being used.

```
12:00:00 | Concurrency: 16 threads (target='dev')
12:00:00 | 
12:00:00 | 1 of 1 START incremental model DBT_MGUINDON.fct_orders... [RUN]
12:00:00 + Initial Run - Using warehouse TRANSFORMING_XL_WH
```
#### Known Issues
When compiling or generating docs, the console reports that dbt is using the incremental run warehouse. It isn't actually so. During these operations, only the target warehouse is activated.

### snowflake_utils.clone_schema ([source](macros/clone_schema.sql))
This macro clones the source schema into the destination schema.

#### Arguments
* `source_schema` (required): The source schema name
* `destination_schema` (required): The destination schema name
* `source_database` (optional): The source database name
* `destination_database` (optional): The destination database name

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
# for multiple arguments, use the dict syntax
dbt run-operation clone_schema --args "{'source_schema': 'analytics', 'destination_schema': 'ci_schema'}"

# set the databases
dbt run-operation clone_schema --args "{'source_schema': 'analytics', 'destination_schema': 'ci_schema', 'source_database': 'production', 'destination_database': 'temp_database'}"
```

### snowflake_utils.drop_schema ([source](macros/drop_schema.sql))
This macro drops a schema in the selected database (defaults to target database if no database is selected).

#### Arguments
* `schema_name` (required): The schema to drop
* `database` (optional): The database name

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
# for multiple arguments, use the dict syntax
dbt run-operation drop_schema --args "{'schema_name': 'customers_temp', 'database': 'production'}"
```

### snowflake_utils.apply_meta_as_tags ([source](macros/apply_meta_as_tags.sql))
This macro applies specific model meta properties as Snowflake tags during on-run-end. This allows you to author Snowflake tags as part of your dbt project.

#### Arguments
* `results` (required): The [on-run-end context object](https://docs.getdbt.com/reference/dbt-jinja-functions/on-run-end-context).

#### Usage

First, configure your dbt model to have the 'database_tags' meta property as shown (tag examples borrowed from [here](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies.html)):

```
schema.yma

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

The above means:
The Snowflake table ACCOUNT will have the tag 'accounting_row_string' set to the value 'visible'.
Its columns ACCOUNT_NAME and ACCOUNT_NUMBER will both have the tag 'accounting_col_string' set to the value 'visible'

The macro must be called as part of on-run-end, so add the following to dbt_project.yml:
```
on-run-end: "{{ snowflake_utils.apply_meta_as_tags(results) }}"
```

#### Tag removal
This macro only seeks to add or update the tags which are specified in dbt. It won't delete tags which are not defined.
If you need this behaviour, it usually comes naturally as dbt drops and recreates tables/views for most materializations.
If you are using the incremental materialization, be aware of this limitation.


----

## Contributions
Contributions to this package are very welcome! Please create issues for bugs or feature requests, or open PRs against `master`.
