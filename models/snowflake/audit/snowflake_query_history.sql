{{- config(materialized='incremental', unique_key='query_id')-}}
{# This job runs to pull data from Snowflake query history. #}
{% if is_incremental() -%}
{{-
  config(
      pre_hook="SET run_start_time = (select greatest(max(end_time),dateadd(minute,-" ~ var('max_load_minutes', 4320) ~ ",current_timestamp)) run_time from {{ this }}); "
  )
}}
{% else -%}
{{-
  config(
      pre_hook="SET run_start_time = (select dateadd(minute,-" ~ var('max_load_minutes', 4320) ~ ",current_timestamp) run_time)"
  )
}}
{% endif -%}
    {# pre_hook="SET run_start_time = (dateadd(minute,-4320,current_timestamp)); "  #}
    {# use this row for first run of the table #}
{#- This statement executes before the DBT create statement does. -#}
{# This approach is required because subqueries are not allowed in function arguments. -}
{# If it's been more than 3 days since the last run, just the last 3 day's results are imported. -#}
{# If the last load was recent, any extra queries will be querying the future, which is fast to run. -#}

SELECT *
FROM (
{%- for i in range(var('max_load_minutes', 4320)) -%}
  {%- if i % var('minutes_per_batch', 30) == 0 -%}
      SELECT qh.query_id bk_snowflake_query_history, qh.*, current_timestamp as load_datetime
      FROM (
        select *
        from table(information_schema.query_history(
          end_time_range_start=>to_timestamp_ltz(dateadd(minute, {{i}}, $run_start_time)),
          end_time_range_end=>dateadd(minute, {{ var('minutes_per_batch', 4320) }}, to_timestamp_ltz(dateadd(minute, {{i}}, $run_start_time))), RESULT_LIMIT => 10000))) qh
      {%- if var('max_load_minutes', 30) - i > var('minutes_per_batch', 30) %}
        UNION ALL
      {% endif -%}
  {%-endif-%}
{%- endfor %}
)
{% if is_incremental() -%}
where end_time >= (select max(end_time) from {{ this }})
{% endif %}