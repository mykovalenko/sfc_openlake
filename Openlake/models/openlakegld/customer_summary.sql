{{ 
  config(
    materialized='iceberg_restapi_table',
    database='OPENLAKE_ICE',
    schema='"openlakegld"',
    alias='"customer_summary"'
  ) 
}}

      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - not_null
      - name: total_transactions
        description: "Total number of orders by customer"
      - name: total_spent
        description: "Total amount daily spent by customer"
      - name: total_loans
        description: "Total number of open load products by customer"
      - name: total_credit_cards
        description: "Total number of open credit cards by customer"
      - name: total_interactions
        description: "Total number of customer interactions by customer"

SELECT * EXCLUDE("updated_at", "ref_pk")
FROM {{ ref('s_accounts') }}
