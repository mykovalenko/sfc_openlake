{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakegld"',
    alias='"customer_interactions"'
  ) 
}}

SELECT * EXCLUDE("updated_at", "ref_pk")
FROM {{ ref('s_customer_interactions') }}
