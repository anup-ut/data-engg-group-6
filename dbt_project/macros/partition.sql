{% macro ch_drop_partition(this_relation, ds_str) %}
  {% if this_relation is not none and ds_str %}
    ALTER TABLE {{ this_relation }} DROP PARTITION '{{ ds_str }}';
  {% endif %}
{% endmacro %}