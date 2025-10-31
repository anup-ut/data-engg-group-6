{# Force dbt to use a single ClickHouse database equal to target.schema #}

{% macro generate_database_name(custom_database_name, node) -%}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {# ClickHouse doesn’t have schemas; return the same as database to avoid concatenation #}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name, node) -%}
  {# Keep model/table names as-is unless you want to prefix them #}
  {% if custom_alias_name is none %}
    {{ return(node.name) }}
  {% else %}
    {{ return(custom_alias_name) }}
  {% endif %}
{%- endmacro %}