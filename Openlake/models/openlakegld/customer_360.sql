{{ 
  config(
    materialized='view',
    database='OPENLAKE_MNG',
    schema='"openlakegld"',
    alias='"customer_360"'
  ) 
}}

-- Customer 360 View: Comprehensive customer profile with all relationships
-- Derived from Gold layer tables for clean lineage

WITH customer_base AS (
    SELECT 
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "address",
        "state",
        "creation_date"
    FROM {{ ref('customers') }}
),

account_metrics AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "account_id") as total_accounts,
        SUM("balance") as total_account_balance,
        MIN("open_date") as first_account_date,
        MAX("open_date") as latest_account_date
    FROM {{ ref('accounts') }}
    GROUP BY "customer_id"
),

loan_metrics AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "loan_id") as total_loans,
        SUM("loan_amount") as total_loan_amount,
        AVG("interest_rate") as avg_interest_rate,
        COUNT(CASE WHEN "status" = 'Active' THEN 1 END) as active_loans
    FROM {{ ref('loans') }}
    GROUP BY "customer_id"
),

credit_card_metrics AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "card_id") as total_credit_cards,
        SUM("credit_limit") as total_credit_limit,
        SUM("current_balance") as total_credit_balance,
        SUM("credit_limit") - SUM("current_balance") as available_credit
    FROM {{ ref('credit_cards') }}
    GROUP BY "customer_id"
),

transaction_metrics AS (
    SELECT 
        a."customer_id",
        COUNT(DISTINCT t."transaction_id") as total_transactions,
        SUM(t."amount") as total_transaction_amount,
        AVG(t."amount") as avg_transaction_amount,
        MAX(t."transaction_date") as last_transaction_date
    FROM {{ ref('transactions') }} t
    INNER JOIN {{ ref('accounts') }} a 
        ON t."account_id" = a."account_id"
    GROUP BY a."customer_id"
),

interaction_metrics AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "interaction_id") as total_interactions,
        MAX("interaction_date") as last_interaction_date
    FROM {{ ref('customer_interactions') }}
    GROUP BY "customer_id"
)

SELECT 
    c."customer_id",
    c."first_name",
    c."last_name",
    c."first_name" || ' ' || c."last_name" as full_name,
    c."email",
    c."phone_number",
    c."address",
    c."state",
    c."creation_date",
    DATEDIFF(day, c."creation_date", CURRENT_DATE()) as days_as_customer,
    
    -- Account Metrics
    COALESCE(am.total_accounts, 0) as total_accounts,
    COALESCE(am.total_account_balance, 0) as total_account_balance,
    am.first_account_date,
    am.latest_account_date,
    
    -- Loan Metrics
    COALESCE(lm.total_loans, 0) as total_loans,
    COALESCE(lm.total_loan_amount, 0) as total_loan_amount,
    lm.avg_interest_rate,
    COALESCE(lm.active_loans, 0) as active_loans,
    
    -- Credit Card Metrics
    COALESCE(ccm.total_credit_cards, 0) as total_credit_cards,
    COALESCE(ccm.total_credit_limit, 0) as total_credit_limit,
    COALESCE(ccm.total_credit_balance, 0) as total_credit_balance,
    COALESCE(ccm.available_credit, 0) as available_credit,
    CASE 
        WHEN ccm.total_credit_limit > 0 
        THEN (ccm.total_credit_balance / ccm.total_credit_limit) * 100 
        ELSE 0 
    END as credit_utilization_pct,
    
    -- Transaction Metrics
    COALESCE(tm.total_transactions, 0) as total_transactions,
    COALESCE(tm.total_transaction_amount, 0) as total_transaction_amount,
    tm.avg_transaction_amount,
    tm.last_transaction_date,
    
    -- Interaction Metrics
    COALESCE(im.total_interactions, 0) as total_interactions,
    im.last_interaction_date,
    
    -- Overall Health Score (based on financial metrics and interactions)
    CASE 
        WHEN ccm.available_credit > 10000 AND lm.active_loans = 0 AND im.total_interactions > 0 THEN 'Excellent'
        WHEN ccm.available_credit > 5000 AND lm.active_loans <= 1 THEN 'Good'
        WHEN ccm.available_credit >= 0 AND im.total_interactions > 0 THEN 'Fair'
        ELSE 'Needs Attention'
    END as customer_health_status,
    
    CURRENT_TIMESTAMP() as last_updated

FROM customer_base c
LEFT JOIN account_metrics am ON c."customer_id" = am."customer_id"
LEFT JOIN loan_metrics lm ON c."customer_id" = lm."customer_id"
LEFT JOIN credit_card_metrics ccm ON c."customer_id" = ccm."customer_id"
LEFT JOIN transaction_metrics tm ON c."customer_id" = tm."customer_id"
LEFT JOIN interaction_metrics im ON c."customer_id" = im."customer_id"
