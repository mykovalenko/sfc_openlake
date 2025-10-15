{{ 
  config(
    materialized='iceberg_restapi_table_incremental',
    database='OPENLAKE_ICE',
    schema='"openlakeslv"',
    alias='"accounts"'
  ) 
}}

SELECT * EXCLUDE(_SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT), _SNOWFLAKE_UPDATED_AT as "updated_at", "account_id" as "ref_pk"
FROM {{ source('psgs', '"accounts"') }}
WHERE _SNOWFLAKE_UPDATED_AT > COALESCE((SELECT MAX("updated_at") FROM {{ this }}), '1900-01-01'::timestamp)
   OR (_SNOWFLAKE_DELETED = TRUE AND "ref_pk" in (SELECT "ref_pk" FROM {{ this }}))
