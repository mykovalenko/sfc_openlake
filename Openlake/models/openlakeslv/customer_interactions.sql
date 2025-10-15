{{ 
  config(
    materialized='iceberg_restapi_table_incremental',
    database='OPENLAKE_ICE',
    schema='"openlakeslv"',
    alias='"customer_interactions"'
  ) 
}}

SELECT * EXCLUDE(_SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT), _SNOWFLAKE_UPDATED_AT as "updated_at", "interaction_id" as "ref_pk"
FROM {{ source('sqls', '"customer_interactions"') }}
WHERE _SNOWFLAKE_UPDATED_AT > COALESCE((SELECT MAX("updated_at") FROM {{ this }}), '1900-01-01'::timestamp)
   OR (_SNOWFLAKE_DELETED = TRUE AND "ref_pk" in (SELECT "ref_pk" FROM {{ this }}))
