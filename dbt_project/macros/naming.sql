{# macros/naming.sql #}

{% macro generate_database_name(custom_database_name, node) -%}
  {# Prefer explicit DB or model schema (which is the DB in ClickHouse), then target.schema #}
  {% set db = custom_database_name or node.config.schema or target.schema %}
  {{ return(db) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {% set db = custom_schema_name or node.config.schema or target.schema %}
  {{ return(db) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name, node) -%}
  {{ return(custom_alias_name if custom_alias_name is not none else node.name) }}
{%- endmacro %}