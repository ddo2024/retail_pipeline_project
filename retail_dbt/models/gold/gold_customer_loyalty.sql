-- models/gold/gold_customer_loyalty.sql
-- Purpose: Summarize customer purchase behavior for loyalty analysis.
--          Reads facts from Silver and enriches with customer + loyalty program from Staging.

-- Step 1 — SELECT: choose identifiers and measures
-- Purpose: define the grain (by customer) and metrics we want.
SELECT
    c.customer_id,                        -- Customer key
    c.customer_name,                      -- Human-readable name from stg_customers
    COALESCE(COUNT(DISTINCT st.transaction_id), 0) AS total_purchases,  -- # of orders
    COALESCE(SUM(st.total_amount), 0.0)   AS total_spent,               -- Spend across orders
    lp.loyalty_program_name               -- Program label from stg_loyalty_programs

-- Step 2 — FROM: start from the customer dimension in staging
-- Purpose: ensure all customers appear, even with no purchases.
FROM {{ ref('silver_customers') }} AS c

-- Step 3 — LEFT JOIN: bring in transactions (Silver)
-- Purpose: attach purchases/spend; left join keeps zero-activity customers.
LEFT JOIN {{ ref('silver_sales_transactions') }} AS st
    ON c.customer_id = st.customer_id

-- Step 4 — LEFT JOIN: bring in loyalty program attributes (Staging)
-- Purpose: translate loyalty_program_id to a readable program name.
LEFT JOIN {{ ref('silver_loyalty_programs') }} AS lp
    ON c.loyalty_program_id = lp.loyalty_program_id

-- Step 5 — GROUP BY: roll up at customer level
-- Purpose: aggregate metrics per unique customer.
GROUP BY
    c.customer_id,
    c.customer_name,
    lp.loyalty_program_name
