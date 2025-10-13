{{ 
  config(
    materialized='iceberg_custom_table',
    database='OPENLAKE_ICE',
    schema='"openlakeslv"',
    alias='"accounts"'
  ) 
}}

with source_data as (
    select "account_id", "customer_id",	"account_type",	"balance", "open_date"
    from openlake.rep_sqls.accounts
)

select *
from source_data