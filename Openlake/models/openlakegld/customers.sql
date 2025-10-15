{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakegld"',
    alias='"customers"'
  ) 
}}

SELECT * EXCLUDE("updated_at", "ref_pk")
FROM {{ ref('s_customers') }}
