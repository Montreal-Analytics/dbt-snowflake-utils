# Snowflake Utils

This [dbt](https://github.com/dbt-labs/dbt-core) package contains Snowflake-specific macros that can be (re)used across dbt projects. It is maintained by Montreal Analytics, a [Datatonic](https://datatonic.com/) Company.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/montreal-analytics/snowflake_utils/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## Prerequisites
Snowflake Utils is compatible with dbt 1.1.0 and later.

----

# Macros

## Warehouse Sizing Macro

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

## Cloning and Dropping Macros

### snowflake_utils.clone_schema ([source](macros/clone_schema.sql))
This macro is a part of the recommended 2-step Cloning Pattern for dbt development, explained in detail [here](2-step_cloning_pattern.md).

This macro clones the source schema into the destination schema and optionally grants ownership over its tables and views to a new owner.

Note: the owner of the schema is the role that executed the command, but if configured, the owner of its sub-objects would be the new_owner_role. This is important for maintaining and replacing clones and is explained in more detail [here](2-step_cloning_pattern.md).

#### Arguments
* `source_schema` (required): The source schema name
* `destination_schema` (required): The destination schema name
* `source_database` (optional): The source database name; default value is your profile's target database.
* `destination_database` (optional): The destination database name; default value is your profile's target database.
* `new_owner_role` (optional): The new ownership role name. If no value is passed, the ownership will remain unchanged.

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
dbt run-operation clone_schema \
  --args "{'source_schema': 'analytics', 'destination_schema': 'ci_schema'}"

# set the databases and new_owner_role
dbt run-operation clone_schema \
  --args "{'source_schema': 'analytics', 'destination_schema': 'ci_schema', 'source_database': 'production', 'destination_database': 'temp_database', 'new_owner_role': 'developer_role'}"
```


### snowflake_utils.clone_database ([source](macros/clone_database.sql))
This macro is a part of the recommended 2-step Cloning Pattern for dbt development, explained in detail [here](2-step_cloning_pattern.md).

This macro clones the source database into the destination database and optionally grants ownership over its schemata and its schemata's tables and views to a new owner.

Note: the owner of the database is the role that executed the command, but if configured, the owner of its sub-objects would be the new_owner_role. This is important for maintaining and replacing clones and is explained in more detail [here](2-step_cloning_pattern.md).

#### Arguments
* `source_database` (required): The source database name
* `destination_database` (required): The destination database name
* `new_owner_role` (optional): The new ownership role name. If no value is passed, the ownership will remain unchanged.

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
dbt run-operation clone_database \
  --args "{'source_database': 'production_clone', 'destination_database': 'developer_clone'}"

# set the new_owner_role
dbt run-operation clone_database \
  --args "{'source_database': 'production_clone', 'destination_database': 'developer_clone', 'new_owner_role': 'developer_role'}"
```

### snowflake_utils.drop_schema ([source](macros/drop_schema.sql))
This macro drops a schema in the selected database (defaults to target database if no database is selected). A schema can only be dropped by the role that owns it.

#### Arguments
* `schema_name` (required): The schema to drop
* `database` (optional): The database name

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
dbt run-operation drop_schema \
  --args "{'schema_name': 'customers_temp', 'database': 'production'}"
```

### snowflake_utils.drop_database ([source](macros/drop_database.sql))
This macro drops a database. A database can only be dropped by the role that owns it.

#### Arguments
* `database_name` (required): The database name

#### Usage

Call the macro as an [operation](https://docs.getdbt.com/docs/using-operations):

```
dbt run-operation drop_database \
  --args "{'database_name': 'production_clone'}"
```

## Tagging Macros
### snowflake_utils.apply_meta_as_tags ([source](macros/apply_meta_as_tags.sql))
This macro applies specific model meta properties as Snowflake tags during on-run-end. This allows you to author Snowflake tags as part of your dbt project.

#### Arguments
* `results` (required): The [on-run-end context object](https://docs.getdbt.com/reference/dbt-jinja-functions/on-run-end-context).

#### Usage

First, configure your dbt model to have the 'database_tags' meta property as shown (tag examples borrowed from [here](https://docs.snowflake.com/en/user-guide/tag-based-masking-policies.html)):

```
schema.yml

models:
  - name: ACCOUNT
    +schema: FINANCE
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

All tags are created in the schema of the model where they are added, by default. In the above example the tags will end up
in the FINANCE schema (name depends on how [DBT has been configured](https://docs.getdbt.com/docs/build/custom-schemas)).
If the tag needs to be created in a different location/referred from different location, the below two environment 
variables need to be added in dbt_project.yml file as below:

```yml
# dbt_project.yml

common_tag_database: 'audit'
common_tag_schema: 'tags'
```
In the above example, the tags will be created in audit.tags .

The macro must be called as part of on-run-end, so add the following to dbt_project.yml:
```
on-run-end: "{{ snowflake_utils.apply_meta_as_tags(results) }}"
```

#### Tag removal
This macro only seeks to add or update the tags which are specified in dbt. It won't delete tags which are not defined.
If you need this behaviour, it usually comes naturally as dbt drops and recreates tables/views for most materializations.
If you are using the incremental materialization, be aware of this limitation.

#### Using cached tags
The macro is called as part of on-run-end. There is a gap between the time **the model run** is complete and **the dbt run** is complete.
During this gap, models which `is_incremental()` is `False` will be without the configured tag.
This behavior can pose challenges, particularly when running big monolithic dbt runs.
To address this issue, you can leverage the previous tag that is cached within a dbt model during this gap.
Add this post hook for each tag you want to use the previous tag during the mentioned gap:
```yml
# dbt_project.yml

...

models:
    my_project:
        +post-hook: "{{ apply_cached_tag(this,'some-tag-name','some-default-value') }}"

```

### snowflake_utils.apply_cached_tag([source](macros/apply_cahed_tag.sql))

This macro applies the previous tag, cached in a dbt model.


#### Arguments
* `this` (required): The database representation of the current model - [this](https://docs.getdbt.com/reference/dbt-jinja-functions/this).
* `tag_name` (required): The name of the tag to apply value from cache.
* `default_value` (required): A default value to be applied if tag not found in cache. Will be used for the first time the model runs.

#### Usage

Add the following post-hook to `dbt_project.yml`:
```yml
# dbt_project.yml

...

models:
    my_project:
        +post-hook: "{{ apply_cached_tag(this,'some-tag-name','some-default-value') }}"

```

This requires:

- Create a base model, materialized as `table`, on top of the view [tag_references](https://docs.snowflake.com/en/sql-reference/account-usage/tag_references).
- Run `dbt run stg_account_usage__tag_references` before each dbt run.



----

## Contributions
Contributions to this package are very welcome! Please create issues for bugs or feature requests, or open PRs against `master`.
