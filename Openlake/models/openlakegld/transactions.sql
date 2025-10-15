{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakegld"',
    alias='"transactions"'
  ) 
}}

SELECT * EXCLUDE("updated_at", "ref_pk")
FROM {{ ref('s_transactions') }}
