{% materialization iceberg_custom_table, adapter='default' %}
  {#-- 1. Set up relations --#}
  {% set target_relation = this %}

  {#-- 2. Build SQL for temp table --#}
  {% set sql -%}
    INSERT INTO {{ target_relation }}
    {{ compiled_code }}
  {%- endset %}

  {#-- 3. Run statement --#}
  {% do run_query(sql) %}

  {#-- 4. Return the final table relation for downstream references --#}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}