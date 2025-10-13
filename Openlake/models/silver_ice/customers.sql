{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakeslv"',
    alias='"customers"'
  ) 
}}

SELECT * EXCLUDE(_SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED), _SNOWFLAKE_UPDATED_AT as "updated_at"
FROM {{ source('psgs', '"customers"') }}
WHERE _SNOWFLAKE_DELETED = FALSE
  AND _SNOWFLAKE_UPDATED_AT > COALESCE((SELECT MAX("updated_at") FROM {{ this }}), '1900-01-01'::timestamp)
