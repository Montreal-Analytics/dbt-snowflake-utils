{% macro apply_meta_as_tags(results) %}
  {{ log('apply_meta_as_tags', info=True) }}
  {{ log(results) }}
  {% if execute %}
    {# 
    -- The tags_by_schema object will act as a local cache of Snowflake tags.
    -- This means we only need to call "show tags in <schema>" once per schema we process.
    #}
    {%- set tags_by_schema = {} -%}
    {% for res in results -%}
        {% if snowflake_utils.model_contains_tag_meta(res.node) %}

            -- Tagging database and schema will be fetched from the below environment variables.
            {%- set tag_database = var('common_tag_database') -%}
            {%- set tag_schema = var('common_tag_schema') -%}
            {%- set tag_schema_full = tag_database+'.'+tag_schema -%}

            {%- set model_database = res.node.database -%}
            {%- set model_schema = res.node.schema -%}
            {%- set model_schema_full = model_database+'.'+model_schema -%}
            {%- set model_alias = res.node.alias -%}

            {%- call statement('set_database', fetch_result=True) -%}
                USE DATABASE {{model_database}}
            {%- endcall -%}

            {%- call statement('set_schema', fetch_result=True) -%}
                USE SCHEMA {{model_schema}}
            {%- endcall -%}

            {% if tag_schema_full not in tags_by_schema.keys() %}
                {{ log('need to fetch tags for schema '+tag_schema_full, info=True) }}
                {%- call statement('main', fetch_result=True) -%}
                    show tags in {{tag_database}}.{{tag_schema}}
                {%- endcall -%}
                {%- set _ = tags_by_schema.update({tag_schema_full: load_result('main')['table'].columns.get('name').values()|list}) -%}
                {{ log('Added tags to cache', info=True) }}
            {% else %}
                {{ log('already have tag info for schema', info=True) }}
            {% endif %}

            {%- set current_tags_in_schema = tags_by_schema[tag_schema_full] -%}
            {{ log('current_tags_in_schema:', info=True) }}
            {{ log(current_tags_in_schema, info=True) }}
            {{ log("========== Processing tags for "+model_schema_full+"."+model_alias+" ==========", info=True) }}
            {% if res.node.meta %}
                {%- set model_meta = res.node.meta -%}
            {% else %}
                {%- set model_meta = res.node.config.meta -%}
            {% endif%}
            {% set line -%}
                node: {{ res.node.unique_id }}; status: {{ res.status }} (message: {{ res.message }})
                model level database tags: {{ model_meta.database_tags}}
                materialized: {{ res.node.config.materialized }}
                database: {{ model_database }}
                schema: {{ model_schema }}
            {%- endset %}
            {{ log(line, info=True) }}
            {#
            -- Uses the tag_references_all_columns table function to fetch existing tags for the table
            #}
            {%- call statement('main', fetch_result=True) -%}
                select LEVEL,OBJECT_NAME,COLUMN_NAME,UPPER(TAG_NAME) as TAG_NAME,TAG_VALUE from table(information_schema.tag_references_all_columns('{{model_alias}}', 'table'))
            {%- endcall -%}
            {%- set existing_tags_for_table = load_result('main')['data'] -%}
            {{ log('Existing tags for table:', info=True) }}
            {{ log(existing_tags_for_table, info=True) }}

            {% for table_tag in model_meta.database_tags %}
                {% set table_tag_full = tag_schema_full+'.'+table_tag %}
                {{ snowflake_utils.create_tag_if_missing(current_tags_in_schema,table_tag_full|upper) }}
                {% set desired_tag_value = model_meta.database_tags[table_tag] %}
                {{ snowflake_utils.set_table_tag_value_if_different(model_alias|upper,table_tag_full|upper,desired_tag_value,existing_tags_for_table) }}
            {% endfor %}
            {% for column in res.node.columns %}
                {% for column_tag in res.node.columns[column].meta.database_tags %}
                    {% set column_tag_full = tag_schema_full+'.'+column_tag %}
                    {{log(column_tag,info=True)}}
                    {{ snowflake_utils.create_tag_if_missing(current_tags_in_schema,column_tag_full|upper)}}
                    {% set desired_tag_value = res.node.columns[column].meta.database_tags[column_tag] %}
                    {{ snowflake_utils.set_column_tag_value_if_different(model_alias|upper,column|upper,column_tag_full|upper,desired_tag_value,existing_tags_for_table)}}
                {% endfor %}
            {% endfor %}
            {{ log("========== Finished processing tags for "+model_alias+" ==========", info=True) }}
        {% endif %}
    {% endfor %}
  {% endif %}
  -- Need to return something other than None, since DBT will try to execute it as SQL statement
  {{ return('') }}
{% endmacro %}

{#
-- Given a node in a Result object, returns True if either the model meta contains database_tags,
-- or any of the column's meta contains database_tags.
-- Otherwise it returns False
#}
{% macro model_contains_tag_meta(model_node) %}
	{% if model_node.meta.database_tags %}
        {{ return(True) }}
	{% endif %}
    {#
    -- For compatibility with the old results structure
    #}
    {% if model_node.config.meta.database_tags %}
        {{ return(True) }}
	{% endif %}
    {% for column in model_node.columns %}
        {% if model_node.columns[column].meta.database_tags %}
            {{ return(True) }}
    	{% endif %}
    {% endfor %}
    {{ return(False) }}
{% endmacro %}

{#
-- Snowflake tags must exist before they are used.
-- Given a list of all the existing tags in the account (all_tag_names),
-- checks if the new tag (new_tag) is already in the list and
-- creates it in Snowflake if it doesn't.
#}
{% macro create_tag_if_missing(all_tag_names,new_tag) %}
	{% if new_tag.split('.')[2] not in all_tag_names %}
		{{ log('Creating missing tag '+new_tag, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            create tag {{new_tag}}
        {%- endcall -%}
        {{ all_tag_names.append(new_tag)}}
		{{ log(load_result('main').data, info=True) }}
	{% else %}
		{{ log('Tag already exists: '+new_tag, info=True) }}
	{% endif %}
{% endmacro %}

-- select LEVEL,OBJECT_NAME,COLUMN_NAME,UPPER(TAG_NAME) as TAG_NAME,TAG_VALUE
{#
-- Updates the value of a Snowflake table tag, if the provided value is different.
-- existing_tags contains the results from querying tag_references_all_columns.
-- The first column (attribute '0') contains 'TABLE' or 'COLUMN', since we're looking
-- at table tags here then we include only 'TABLE' values.
-- The second column (attribute '1') contains the name of the table, we filter on that.
-- The third column (attribute '2') contains the name of the column, not relevant here.
-- The fourth column (attribute '3') contains the tag name, so we filter on that too.
-- The fifth column (index 4) contains the value of the tag, so we compare with the desired_tag_value
-- to see if we need to update it
#}
{% macro set_table_tag_value_if_different(table_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at table level', info=True) }}
    {{ log(existing_tags, info=True) }}
    {%- set existing_tag_for_table = existing_tags|selectattr('0','equalto','TABLE')|selectattr('1','equalto',table_name|upper)|selectattr('3','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for table:', info=True) }}
    {{ log(existing_tag_for_table, info=True) }}
    {% if existing_tag_for_table|length > 0 and existing_tag_for_table[0][4]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=True) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
    {% endif %}
{% endmacro %}
{#
-- Updates the value of a Snowflake column tag, if the provided value is different.
-- existing_tags contains the results from querying tag_references_all_columns.
-- The first column (attribute '0') contains 'TABLE' or 'COLUMN', since we're looking
-- at column tags here then we include only 'COLUMN' values.
-- The second column (attribute '1') contains the name of the table, we filter on that.
-- The third column (attribute '2') contains the name of the column, we filter on that.
-- The fourth column (attribute '3') contains the tag name, so we filter on that too.
-- The fifth column (index 4) contains the value of the tag, so we compare with the desired_tag_value
-- to see if we need to update it
#}
{% macro set_column_tag_value_if_different(table_name,column_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at column level', info=True) }}
    {%- set existing_tag_for_column = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',table_name|upper)|selectattr('2','equalto',column_name|upper)|selectattr('3','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for column:', info=True) }}
    {{ log(existing_tag_for_column, info=True) }}
    {% if existing_tag_for_column|length > 0 and existing_tag_for_column[0][4]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=True) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} modify column {{column_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
    {% endif %}
{% endmacro %}