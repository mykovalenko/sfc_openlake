{{ 
  config(
    materialized='view',
    database='OPENLAKE_MNG',
    schema='"openlakegld"',
    alias='"customer_risk_profile"'
  ) 
}}

-- Customer Risk Profile: Credit risk assessment based on Gold layer tables
-- Clean lineage: Gold Tables → Analytics View in OPENLAKE_MNG

WITH customer_base AS (
    SELECT 
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "state",
        "creation_date"
    FROM {{ ref('customers') }}
),

loan_risk AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "loan_id") as total_loans,
        SUM("loan_amount") as total_loan_exposure,
        COUNT(CASE WHEN "loan_amount" > 100000 THEN 1 END) as high_value_loans,
        AVG("interest_rate") as avg_loan_interest_rate,
        MAX("interest_rate") as max_loan_interest_rate,
        COUNT(CASE WHEN "status" = 'Default' THEN 1 END) as defaulted_loans,
        COUNT(CASE WHEN "status" = 'Active' THEN 1 END) as active_loans
    FROM {{ ref('loans') }}
    GROUP BY "customer_id"
),

credit_card_risk AS (
    SELECT 
        "customer_id",
        COUNT(DISTINCT "card_id") as total_cards,
        SUM("credit_limit") as total_credit_limit,
        SUM("current_balance") as total_credit_balance,
        CASE 
            WHEN SUM("credit_limit") > 0 
            THEN (SUM("current_balance") / SUM("credit_limit")) * 100 
            ELSE 0 
        END as credit_utilization_pct,
        COUNT(CASE WHEN ("current_balance" / NULLIF("credit_limit", 0)) > 0.9 THEN 1 END) as maxed_out_cards
    FROM {{ ref('credit_cards') }}
    GROUP BY "customer_id"
),

account_balance AS (
    SELECT 
        "customer_id",
        SUM("balance") as total_account_balance,
        COUNT(CASE WHEN "balance" < 0 THEN 1 END) as negative_balance_accounts
    FROM {{ ref('accounts') }}
    GROUP BY "customer_id"
),

transaction_behavior AS (
    SELECT 
        a."customer_id",
        COUNT(DISTINCT t."transaction_id") as total_transactions,
        SUM(t."amount") as total_transaction_volume,
        AVG(t."amount") as avg_transaction_amount,
        STDDEV(t."amount") as transaction_volatility
    FROM {{ ref('transactions') }} t
    INNER JOIN {{ ref('accounts') }} a 
        ON t."account_id" = a."account_id"
    GROUP BY a."customer_id"
)

SELECT 
    c."customer_id",
    c."first_name" || ' ' || c."last_name" as customer_name,
    c."email",
    c."state",
    
    -- Loan Risk Indicators
    COALESCE(lr.total_loans, 0) as total_loans,
    COALESCE(lr.total_loan_exposure, 0) as total_loan_exposure,
    COALESCE(lr.high_value_loans, 0) as high_value_loans,
    COALESCE(lr.defaulted_loans, 0) as defaulted_loans,
    COALESCE(lr.active_loans, 0) as active_loans,
    lr.avg_loan_interest_rate,
    lr.max_loan_interest_rate,
    
    -- Credit Card Risk Indicators
    COALESCE(ccr.total_cards, 0) as total_credit_cards,
    COALESCE(ccr.total_credit_limit, 0) as total_credit_limit,
    COALESCE(ccr.total_credit_balance, 0) as total_credit_balance,
    ccr.credit_utilization_pct,
    COALESCE(ccr.maxed_out_cards, 0) as maxed_out_cards,
    
    -- Account Balance Indicators
    COALESCE(ab.total_account_balance, 0) as total_account_balance,
    COALESCE(ab.negative_balance_accounts, 0) as negative_balance_accounts,
    
    -- Transaction Behavior
    COALESCE(tb.total_transactions, 0) as total_transactions,
    COALESCE(tb.total_transaction_volume, 0) as total_transaction_volume,
    tb.avg_transaction_amount,
    tb.transaction_volatility,
    
    -- Risk Score Calculation (0-100, higher = more risk)
    LEAST(100, (
        (COALESCE(lr.defaulted_loans, 0) * 25) +
        (CASE WHEN ccr.credit_utilization_pct > 80 THEN 15 ELSE 0 END) +
        (COALESCE(ccr.maxed_out_cards, 0) * 10) +
        (COALESCE(ab.negative_balance_accounts, 0) * 5) +
        (CASE WHEN lr.avg_loan_interest_rate > 10 THEN 10 ELSE 0 END) +
        (CASE WHEN ab.total_account_balance < 1000 THEN 10 ELSE 0 END)
    )) as risk_score,
    
    -- Risk Category
    CASE 
        WHEN (
            (COALESCE(lr.defaulted_loans, 0) * 25) +
            (CASE WHEN ccr.credit_utilization_pct > 80 THEN 15 ELSE 0 END) +
            (COALESCE(ccr.maxed_out_cards, 0) * 10) +
            (COALESCE(ab.negative_balance_accounts, 0) * 5) +
            (CASE WHEN lr.avg_loan_interest_rate > 10 THEN 10 ELSE 0 END) +
            (CASE WHEN ab.total_account_balance < 1000 THEN 10 ELSE 0 END)
        ) >= 50 THEN 'High Risk'
        WHEN (
            (COALESCE(lr.defaulted_loans, 0) * 25) +
            (CASE WHEN ccr.credit_utilization_pct > 80 THEN 15 ELSE 0 END) +
            (COALESCE(ccr.maxed_out_cards, 0) * 10) +
            (COALESCE(ab.negative_balance_accounts, 0) * 5) +
            (CASE WHEN lr.avg_loan_interest_rate > 10 THEN 10 ELSE 0 END) +
            (CASE WHEN ab.total_account_balance < 1000 THEN 10 ELSE 0 END)
        ) >= 25 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category,
    
    -- Red Flags
    CASE WHEN lr.defaulted_loans > 0 THEN TRUE ELSE FALSE END as has_loan_defaults,
    CASE WHEN ccr.credit_utilization_pct > 90 THEN TRUE ELSE FALSE END as high_credit_utilization,
    CASE WHEN ab.negative_balance_accounts > 0 THEN TRUE ELSE FALSE END as has_overdrafts,
    CASE WHEN lr.high_value_loans > 0 THEN TRUE ELSE FALSE END as has_high_value_loans,
    
    -- Recommendation
    CASE 
        WHEN lr.defaulted_loans > 0 THEN 'Deny New Credit - Default History'
        WHEN ccr.credit_utilization_pct > 90 AND ab.total_account_balance < 1000 THEN 'High Risk - Monitor Closely'
        WHEN ccr.credit_utilization_pct > 70 THEN 'Review Credit Limit'
        WHEN ab.negative_balance_accounts > 0 THEN 'Overdraft Protection Needed'
        ELSE 'Approve New Credit'
    END as credit_recommendation,
    
    CURRENT_TIMESTAMP() as calculated_at

FROM customer_base c
LEFT JOIN loan_risk lr ON c."customer_id" = lr."customer_id"
LEFT JOIN credit_card_risk ccr ON c."customer_id" = ccr."customer_id"
LEFT JOIN account_balance ab ON c."customer_id" = ab."customer_id"
LEFT JOIN transaction_behavior tb ON c."customer_id" = tb."customer_id"
