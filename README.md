# Snowflake Utils

This [dbt](https://github.com/fishtown-analytics/dbt) package contains Snowflake-specific macros that can be (re)used across dbt projects.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/fishtown-analytics/snowflake-utils/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

----

## Macros

### warehouse_size() ([source](macros/warehouse_size.sql))
This macro returns an alternative warehouse if conditions are met. It will, in order, check the following conditions for incremental models:

- The relation doesn't exist (initial run) _and_ a warehouse has been configured
- Full refresh run _and_ a warehouse has been configured
- Incremental run _and_ a warehouse has been configured

Otherwise, it returns the target warehouse configured in the profile.

#### Usage

Call the macro from the `snowflake_warehouse` model configuration:
```
{{ 
    config(
      snowflake_warehouse=warehouse_size()
    )
}}
```

#### Macro Configuration

Out-of-the-box, the macro will return the `target.warehouse` for each condition, unless exceptions are configured using one or more of the following [variables](https://docs.getdbt.com/docs/using-variables):

| variable | information | required |
|----------|-------------|:--------:|
|snowflake-utils:initial_run_warehouse|Alternative warehouse when the relation doesn't exist|No|
|snowflake-utils:full_refresh_run_warehouse|Alternative warehouse when doing a `--full-refresh`|No|
|snowflake-utils:incremental_run_warehouse|Alternative warehouse to use for incremental runs|No|

An example `dbt_project.yml` configuration:

```yml
# dbt_project.yml

...

models:
    my_project:
        vars:
            'snowflake-utils:full_run_warehouse': "transforming_xl_wh"


```



