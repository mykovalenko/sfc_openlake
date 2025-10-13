{% materialization iceberg_restapi_table, adapter='default' %}
  {% set target_relation = this %}

  {% set sql_stmt -%}
    INSERT INTO {{ target_relation }}
    {{ compiled_code }}
  {%- endset %}

  {{ run_hooks(pre_hooks) }}
  {% call statement('main') %} {{ sql_stmt }} {% endcall %}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
