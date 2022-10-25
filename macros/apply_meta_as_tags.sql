{% macro apply_meta_as_tags(results) %}
  {{ log('apply_meta_as_tags', info=True) }}
  {{ log(results) }}
  {% if execute %}
    
    {%- set tags_by_schema = {} -%}
    {% for res in results -%}
        {% if res.node.meta.database_tags %}
            
            {%- set model_database = res.node.database -%}
            {%- set model_schema = res.node.schema -%}
            {%- set model_schema_full = model_database+'.'+model_schema -%}
            {%- set model_alias = res.node.alias -%}

            {% if model_schema_full not in tags_by_schema.keys() %}
                {{ log('need to fetch tags for schema '+model_schema_full, info=True) }}
                {%- call statement('main', fetch_result=True) -%}
                    show tags in {{model_database}}.{{model_schema}}
                {%- endcall -%}
                {%- set _ = tags_by_schema.update({model_schema_full: load_result('main')['table'].columns.get('name').values()|list}) -%}
                {{ log('Added tags to cache', info=True) }}
            {% else %}
                {{ log('already have tag info for schema', info=True) }}
            {% endif %}

            {%- set current_tags_in_schema = tags_by_schema[model_schema_full] -%}
            {{ log('current_tags_in_schema:', info=True) }}
            {{ log(current_tags_in_schema, info=True) }}
            {{ log("========== Processing tags for "+model_schema_full+"."+model_alias+" ==========", info=True) }}

            {% set line -%}
                node: {{ res.node.unique_id }}; status: {{ res.status }} (message: {{ res.message }})
                database tags: {{ res.node.meta.database_tags}}
                materialized: {{ res.node.config.materialized }}
            {%- endset %}
            {{ log(line, info=True) }}

            {%- call statement('main', fetch_result=True) -%}
                select LEVEL,UPPER(TAG_NAME) as TAG_NAME,TAG_VALUE from table(information_schema.tag_references_all_columns('{{model_alias}}', 'table'))
            {%- endcall -%}
            {%- set existing_tags_for_table = load_result('main')['data'] -%}
            {{ log('Existing tags for table:', info=True) }}
            {{ log(existing_tags_for_table, info=True) }}

            {{ log('--', info=True) }}
            {% for table_tag in res.node.meta.database_tags %}

                {{ create_tag_if_missing(current_tags_in_schema,table_tag|upper) }}
                {% set desired_tag_value = res.node.meta.database_tags[table_tag] %}

                {{set_table_tag_value_if_different(model_alias,table_tag,desired_tag_value,existing_tags_for_table)}}
            {% endfor %}
            {% for column in res.node.columns %}
                {% for column_tag in res.node.columns[column].meta.database_tags %}
                    {{log(column_tag,info=True)}}
                    {{create_tag_if_missing(current_tags_in_schema,column_tag|upper)}}
                    {% set desired_tag_value = res.node.columns[column].meta.database_tags[column_tag] %}
                    {{set_column_tag_value_if_different(model_alias,column,column_tag,desired_tag_value,existing_tags_for_table)}}
                {% endfor %}
            {% endfor %}
            {{ log("========== Finished processing tags for "+model_alias+" ==========", info=True) }}
        {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}


{% macro create_tag_if_missing(all_tag_names,table_tag) %}
	{% if table_tag not in all_tag_names %}
		{{ log('Creating missing tag '+table_tag, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            create tag {{table_tag}}
        {%- endcall -%}
		{{ log(load_result('main').data, info=True) }}
	{% else %}
		{{ log('Tag already exists: '+table_tag, info=True) }}
	{% endif %}
{% endmacro %}

{% macro set_table_tag_value_if_different(table_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at table level', info=True) }}
    {%- set existing_tag_for_table = existing_tags|selectattr('0','equalto','TABLE')|selectattr('1','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for table:', info=True) }}
    {{ log(existing_tag_for_table[0], info=True) }}
    {% if existing_tag_for_table|length > 0 and existing_tag_for_table[0][2]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=True) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
    {% endif %}
{% endmacro %}

{% macro set_column_tag_value_if_different(table_name,column_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at column level', info=True) }}
    {%- set existing_tag_for_column = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for column:', info=True) }}
    {{ log(existing_tag_for_column[0], info=True) }}
    {% if existing_tag_for_column|length > 0 and existing_tag_for_column[0][2]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=True) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} modify column {{column_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
    {% endif %}
{% endmacro %}