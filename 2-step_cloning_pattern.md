# 2-Step dbt Cloning Pattern

_Credit: [This cloning pattern is inspired by Dan Gooden’s article here from the Airtasker Tribe blog.](https://medium.com/airtribe/test-sql-pipelines-against-production-clones-using-dbt-and-snowflake-2f8293722dd4)_

Cloning is a cost- and time-efficient way of developing dbt models on Snowflake but it can be challenging when your cloning needs traverse different environments with different access controls: i.e. you want to clone a production database for use in development. 

A solution for this is to run a 2-step cloning pattern:

1. A production role clones the production database or schema and then changes the ownership of its sub-objects to a developer role, thus creating a developer clone of production. The cloned object is still owned by the production role (which preserves the privilege to drop or replace that clone), but now the developer role has full access of its sub-objects.
2. Developer users use the developer role to clone that developer clone database or schema, thus creating a new personal developer clone for development. The developer role has full ownership of this cloned database and all its sub-objects.

This pattern can be used for cloning a schema or a database. If all the dbt models are stored within a single schema, schema-level cloning is a good option. When dbt is configured to write data to multiple schemata, database-level cloning is a good, more production-like option.

This patterns optimizes for the following:

- **Access Control:** no need to compromise on your access control system, such as by allowing your developer role to have extensive access on production. This pattern takes environmental separation as a given.
- **Flexible Availability:** step 1 can be run on any preferred schedule: the developer clone could be updated hourly, daily, weekly, or any other cadence. This first clone is ideally run after a complete execution of dbt for the freshest data possible.
- **Developer Flexibility:** developers can take personal clones whenever they need to and can even take multiple clones if they have need of more than one concurrent development environment. These developer clones are ideally commonly rotated to keep data fresh and production-like.

## Setup:

1. Update one of your production jobs to include step 1 of the cloning pattern. Here is an example implementation for database-level cloning from production to production_clone:
    
    ```bash
    dbt build &&
    dbt run-operation clone_database \
      --args "{'source_database': 'production', 'destination_database': 'production_clone', 'new_owner_role': 'developer_role'}"
    ```
    
2. As needed, locally run step 2 of the cloning pattern to create or update personal development clones. Here is an example implementation for database-level cloning from production_clone to an ephemeral database called developer_clone_me:
    
    ```bash
    dbt run-operation clone_database \
      --args "{'source_database': 'production_clone', 'destination_database': 'developer_clone_me'}"
    ```
