# Snowflake Utils

This [dbt](https://github.com/fishtown-analytics/dbt) package contains Snowflake-specific macros that can be (re)used across dbt projects.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/fishtown-analytics/snowflake_utils/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Prerequisites
Snowflake Utils is compatible with dbt 0.15.0 and later.

----

## Macros

### snowflake_utils.warehouse_size() ([source](macros/warehouse_size.sql))
This macro returns an alternative warehouse if conditions are met. It will, in order, check the following conditions for incremental models:

- The relation doesn't exist (initial run) _and_ a warehouse has been configured
- Full refresh run _and_ a warehouse has been configured

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

### snowflake_utils.clone_database ([source](macros/clone_database.sql))
This macro clones the source database into the destination database.  The destination database must already exist.  Existing tables in the target will be replaced if already present, but will not be dropped if they no longer exist in the source database.


#### Arguments
* `source_database` (required): The source database name
* `destination_database` (required): The destination database name
* `exclude_schemas` (optional): List of schemas to exclude from cloning

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
dbt run-operation clone_database --args "{'source_database': 'production', 'destination_database': 'temp_database'}"

# Excluding schemas
dbt run-operation clone_database --args "{'source_database': 'production', 'destination_database': 'temp_database', 'exclude_schemas': ['large_legacy_data', 'large_legacy_data2']}"

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

----

## Contributions
Contributions to this package are very welcome! Please create issues for bugs or feature requests, or open PRs against `master`.
