{# Force dbt to keep database == schema for ClickHouse #}
{% macro generate_database_name(custom_database_name, node) -%}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name, node) -%}
  {% if custom_alias_name is none %}
    {{ return(node.name) }}
  {% else %}
    {{ return(custom_alias_name) }}
  {% endif %}
{%- endmacro %}