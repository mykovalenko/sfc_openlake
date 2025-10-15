{{ 
  config(
    materialized='view',
    database='OPENLAKE_MNG',
    schema='"openlakegld"',
    alias='"account_performance"'
  ) 
}}

-- Account Performance: Analysis derived from Gold layer tables
-- Clean lineage: Gold Tables → Analytics View in OPENLAKE_MNG

WITH account_base AS (
    SELECT 
        a."account_id",
        a."customer_id",
        a."account_type",
        a."balance",
        a."open_date",
        c."first_name" || ' ' || c."last_name" as customer_name,
        c."state" as customer_state
    FROM {{ ref('accounts') }} a
    INNER JOIN {{ ref('customers') }} c 
        ON a."customer_id" = c."customer_id"
),

transaction_summary AS (
    SELECT 
        "account_id",
        COUNT(DISTINCT "transaction_id") as total_transactions,
        SUM("amount") as total_transaction_amount,
        AVG("amount") as avg_transaction_amount,
        MIN("amount") as min_transaction_amount,
        MAX("amount") as max_transaction_amount,
        STDDEV("amount") as transaction_amount_stddev,
        MIN("transaction_date") as first_transaction_date,
        MAX("transaction_date") as last_transaction_date,
        COUNT(CASE WHEN "amount" > 0 THEN 1 END) as credit_transactions,
        COUNT(CASE WHEN "amount" < 0 THEN 1 END) as debit_transactions,
        SUM(CASE WHEN "amount" > 0 THEN "amount" ELSE 0 END) as total_credits,
        SUM(CASE WHEN "amount" < 0 THEN ABS("amount") ELSE 0 END) as total_debits
    FROM {{ ref('transactions') }}
    GROUP BY "account_id"
),

recent_activity AS (
    SELECT 
        "account_id",
        COUNT(DISTINCT "transaction_id") as transactions_last_30_days,
        SUM("amount") as amount_last_30_days
    FROM {{ ref('transactions') }}
    WHERE "transaction_date" >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY "account_id"
),

account_age_days AS (
    SELECT 
        "account_id",
        DATEDIFF(day, "open_date", CURRENT_DATE()) as days_since_opening
    FROM {{ ref('accounts') }}
)

SELECT 
    ab."account_id",
    ab."customer_id",
    ab.customer_name,
    ab.customer_state,
    ab."account_type",
    ab."balance" as current_balance,
    ab."open_date",
    aad.days_since_opening,
    
    -- Transaction Volume Metrics
    COALESCE(ts.total_transactions, 0) as total_transactions,
    COALESCE(ts.total_transaction_amount, 0) as total_transaction_amount,
    ts.avg_transaction_amount,
    ts.min_transaction_amount,
    ts.max_transaction_amount,
    ts.transaction_amount_stddev,
    
    -- Transaction Activity Breakdown
    COALESCE(ts.credit_transactions, 0) as credit_transactions,
    COALESCE(ts.debit_transactions, 0) as debit_transactions,
    COALESCE(ts.total_credits, 0) as total_credits,
    COALESCE(ts.total_debits, 0) as total_debits,
    COALESCE(ts.total_credits, 0) - COALESCE(ts.total_debits, 0) as net_flow,
    
    -- Transaction Dates
    ts.first_transaction_date,
    ts.last_transaction_date,
    DATEDIFF(day, ts.last_transaction_date, CURRENT_DATE()) as days_since_last_transaction,
    
    -- Recent Activity (Last 30 days)
    COALESCE(ra.transactions_last_30_days, 0) as transactions_last_30_days,
    COALESCE(ra.amount_last_30_days, 0) as amount_last_30_days,
    
    -- Performance Metrics
    CASE 
        WHEN aad.days_since_opening > 0 
        THEN COALESCE(ts.total_transactions, 0) / aad.days_since_opening 
        ELSE 0 
    END as avg_transactions_per_day,
    
    CASE 
        WHEN aad.days_since_opening > 0 
        THEN COALESCE(ts.total_transaction_amount, 0) / aad.days_since_opening 
        ELSE 0 
    END as avg_amount_per_day,
    
    -- Activity Status
    CASE 
        WHEN ts.last_transaction_date IS NULL THEN 'Dormant - No Transactions'
        WHEN DATEDIFF(day, ts.last_transaction_date, CURRENT_DATE()) > 90 THEN 'Inactive - 90+ Days'
        WHEN DATEDIFF(day, ts.last_transaction_date, CURRENT_DATE()) > 30 THEN 'Low Activity - 30-90 Days'
        WHEN ra.transactions_last_30_days >= 10 THEN 'Highly Active'
        WHEN ra.transactions_last_30_days >= 5 THEN 'Active'
        ELSE 'Moderate Activity'
    END as activity_status,
    
    -- Balance Health
    CASE 
        WHEN ab."balance" < 0 THEN 'Overdrawn'
        WHEN ab."balance" < 100 THEN 'Low Balance'
        WHEN ab."balance" < 1000 THEN 'Medium Balance'
        WHEN ab."balance" < 10000 THEN 'Healthy Balance'
        ELSE 'High Balance'
    END as balance_category,
    
    -- Account Value Score (0-100)
    LEAST(100, (
        (CASE WHEN ab."balance" > 10000 THEN 30 
              WHEN ab."balance" > 5000 THEN 20 
              WHEN ab."balance" > 1000 THEN 10 
              ELSE 0 END) +
        (CASE WHEN COALESCE(ra.transactions_last_30_days, 0) >= 10 THEN 30
              WHEN COALESCE(ra.transactions_last_30_days, 0) >= 5 THEN 20
              WHEN COALESCE(ra.transactions_last_30_days, 0) >= 1 THEN 10
              ELSE 0 END) +
        (CASE WHEN aad.days_since_opening > 365 THEN 20
              WHEN aad.days_since_opening > 180 THEN 15
              WHEN aad.days_since_opening > 90 THEN 10
              ELSE 5 END) +
        (CASE WHEN COALESCE(ts.total_credits, 0) > COALESCE(ts.total_debits, 0) THEN 20
              ELSE 10 END)
    )) as account_value_score,
    
    -- Volatility Flag
    CASE 
        WHEN ts.transaction_amount_stddev > 1000 THEN 'High Volatility'
        WHEN ts.transaction_amount_stddev > 500 THEN 'Medium Volatility'
        WHEN ts.transaction_amount_stddev > 0 THEN 'Low Volatility'
        ELSE 'No Activity'
    END as transaction_volatility,
    
    CURRENT_TIMESTAMP() as calculated_at

FROM account_base ab
LEFT JOIN transaction_summary ts ON ab."account_id" = ts."account_id"
LEFT JOIN recent_activity ra ON ab."account_id" = ra."account_id"
LEFT JOIN account_age_days aad ON ab."account_id" = aad."account_id"
