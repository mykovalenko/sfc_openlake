{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakegld"',
    alias='"accounts"'
  ) 
}}

SELECT * EXCLUDE("updated_at", "ref_pk")
FROM {{ source('psgs', '"accounts"') }}
