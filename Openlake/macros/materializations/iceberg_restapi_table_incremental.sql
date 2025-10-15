{% materialization iceberg_restapi_table_incremental, adapter='default' %}
  {% set target_relation = this %}

  {# Run pre-hooks if needed #}
  {{ run_hooks(pre_hooks) }}

  {# DELETE statement #}
  {% call statement('delete_existing', fetch_result=False) %}
    DELETE FROM {{ target_relation }} t
    USING (
        {{ compiled_code }}
    ) as rep
    WHERE
            t."ref_pk" = rep."ref_pk"
        and rep._SNOWFLAKE_DELETED = TRUE
  {% endcall %}

  {# INSERT statement #}
  {% call statement('main', fetch_result=False) %}
    INSERT INTO {{ target_relation }}
    WITH rep AS (
        {{ compiled_code }}
    )
    SELECT * EXCLUDE(_SNOWFLAKE_DELETED)
    FROM rep
    WHERE rep._SNOWFLAKE_DELETED = FALSE
  {% endcall %}

  {# Run post-hooks and cleanup #}
  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
