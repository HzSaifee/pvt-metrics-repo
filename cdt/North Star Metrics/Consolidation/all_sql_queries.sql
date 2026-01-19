-- Unique Accounts using Tooling for Migration by Customer : Initial vs. Phase X Deployments
WITH 
-- 1. GENERATE CALENDAR (Ensures every month exists)
calendar AS (
    SELECT CAST(date_column AS DATE) as month_start
    FROM (
        VALUES 
            (DATE_TRUNC('month', DATE_ADD('month', -1, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -2, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -3, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -4, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -5, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)))
    ) AS t(date_column)
),

-- 2. FILTER POPULATION (Logic from Query 1)
active_population AS (
    SELECT 
        ct.billing_id,
        ct.sf_id AS customer_sf_account_id,
        MIN(DATE_TRUNC('month', CAST(mel.wd_event_date AS DATE))) AS first_active_month
    FROM swh.migration_event_log mel
    INNER JOIN swh.scopes_metrics sm 
        ON mel.source_object_id = sm.scope_external_id
        -- Partition Pruning
        AND sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
    INNER JOIN lookup_db.sfdc_customer_tenants ct 
        ON sm.tenant_name = ct.tenant_name
    WHERE mel.user_type = 'Customer'       -- Specific filter from Query 1
      AND mel.event_type = 'push_migration'   -- Specific filter from Query 1
      AND mel.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND mel.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
    GROUP BY ct.billing_id, ct.sf_id
),

-- 3. CLASSIFY EACH CUSTOMER (Logic from Query 1)
classified_billing_ids AS (
    SELECT 
        ap.first_active_month,
        ap.billing_id,
        CASE 
            WHEN COUNT(CASE WHEN d.type != 'Initial Deployment' THEN 1 END) > 0 
            THEN 'Phase X Deployment'
            ELSE 'Initial Deployment' 
        END AS deployment_bucket
    FROM active_population ap
    INNER JOIN lookup_db.sfdc_deployments d 
        ON ap.customer_sf_account_id = d.customer_sf_account_id
    GROUP BY ap.first_active_month, ap.billing_id
),

-- 4. AGGREGATE PER MONTH
monthly_aggregates AS (
    SELECT 
        first_active_month AS month_start,
        deployment_bucket,
        COUNT(*) AS new_billing_ids
    FROM classified_billing_ids
    GROUP BY 1, 2
),

-- 5. MERGE WITH CALENDAR (The Fix)
--    We Cross Join the calendar with buckets, then Left Join the data.
--    This fills in "0" for any missing month so the math doesn't break.
full_dataset AS (
    SELECT 
        c.month_start,
        b.deployment_bucket,
        COALESCE(ma.new_billing_ids, 0) AS new_billing_ids
    FROM calendar c
    CROSS JOIN (VALUES ('Initial Deployment'), ('Phase X Deployment')) AS b(deployment_bucket)
    LEFT JOIN monthly_aggregates ma 
        ON c.month_start = ma.month_start 
        AND b.deployment_bucket = ma.deployment_bucket
)

-- 6. FINAL WINDOW CALCULATION
SELECT 
    date_trunc('month', CAST(month_start AS TIMESTAMP)) AS "Month",
    deployment_bucket AS "Deployment Type",
    SUM(new_billing_ids) OVER (
        PARTITION BY deployment_bucket 
        ORDER BY month_start 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "MAX(Total Billing IDs)"
FROM full_dataset
ORDER BY "Month" DESC, "Deployment Type";

-- ------------------------------------------------------------------------------------------------
-- Penetration Percentage for Customer Users
WITH Parameters AS (
    -- Centralized configuration
    SELECT 
        6 AS lookback_months,
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS period_start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS current_month_start
),
Denominator AS (
    -- Total Active Production Tenants as of the end of last month
    SELECT 
        COUNT(DISTINCT sf_id) AS total_active_tenants
    FROM lookup_db.sfdc_customer_tenants
    WHERE tenant_type = 'Production'
      AND status = 'Active'
      AND tenant_start_date < DATE_TRUNC('month', CURRENT_DATE)
      AND (
           tenant_expire_date IS NULL 
           OR tenant_expire_date >= DATE_TRUNC('month', CURRENT_DATE)
      )
),
Numerator AS (
    -- Total unique customers whose FIRST migration was within the last 6 months
    SELECT 
        COUNT(DISTINCT fme.tenant_prefix) AS total_unique_migrated_customers
    FROM (
        SELECT 
            cat.tenant_prefix,
            MIN(DATE_TRUNC('month', DATE(mel.wd_event_date))) AS first_migration_month
        FROM swh.migration_event_log AS mel
        INNER JOIN swh.scopes_input_type_metrics AS sitm
            ON mel.source_object_id = sitm.scope_external_id
            AND sitm.wd_event_date IS NOT NULL
        JOIN lookup_db.sfdc_customer_account_tenants cat 
            ON mel.source_tenant = cat.tenant_name 
        WHERE mel.user_type = 'Customer'
          AND mel.wd_event_date IS NOT NULL
        GROUP BY 1
    ) fme
    CROSS JOIN Parameters p
    WHERE fme.first_migration_month >= p.period_start_date
      AND fme.first_migration_month < p.current_month_start
)
SELECT 
    n.total_unique_migrated_customers AS numerator,
    d.total_active_tenants AS denominator,
    ROUND(CAST(n.total_unique_migrated_customers AS DOUBLE) / d.total_active_tenants * 100, 2) AS adoption_percentage
FROM Numerator n
CROSS JOIN Denominator d;

-- ------------------------------------------------------------------------------------------------
-- Total Number of Active Customers
SELECT 
    COUNT(DISTINCT sf_id) AS customer_account_tenants
FROM lookup_db.sfdc_customer_tenants
WHERE tenant_type = 'Production'
  AND status = 'Active'
  AND tenant_start_date < DATE_TRUNC('month', CURRENT_DATE)
  AND (
       tenant_expire_date IS NULL 
       OR tenant_expire_date >= DATE_TRUNC('month', CURRENT_DATE)
  );

-- ------------------------------------------------------------------------------------------------
-- Unique Customer Change Tracker Migrations Usage ME/LE
SELECT 
    date_trunc('month', CAST(month_start AS TIMESTAMP)) AS migration_month,
    -- The Cumulative Window Function: Sum of this month + all previous months
    SUM(new_me_customers) OVER (
        ORDER BY month_start ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "ME Customers",
    SUM(new_le_customers) OVER (
        ORDER BY month_start ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "LE Customers"
FROM (
    WITH 
    -- 1. PARAMETERS: Define the 6-month window
    Parameters AS (
        SELECT 
            DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
            DATE_TRUNC('month', CURRENT_DATE) AS end_date
    ),

    -- 2. CALENDAR GENERATOR: Ensures every month exists (even if counts are 0)
    Calendar AS (
        SELECT CAST(date_column AS DATE) as month_start
        FROM (
            VALUES 
                (DATE_TRUNC('month', DATE_ADD('month', -1, CURRENT_DATE))),
                (DATE_TRUNC('month', DATE_ADD('month', -2, CURRENT_DATE))),
                (DATE_TRUNC('month', DATE_ADD('month', -3, CURRENT_DATE))),
                (DATE_TRUNC('month', DATE_ADD('month', -4, CURRENT_DATE))),
                (DATE_TRUNC('month', DATE_ADD('month', -5, CURRENT_DATE))),
                (DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)))
        ) AS t(date_column)
    ),

    -- 3. RAW DATA: Identify First Migration Month per Tenant
    FirstMigrationEver AS (
        SELECT 
            cat.tenant_prefix,
            sad.enterprise_size_group,
            MIN(DATE_TRUNC('month', DATE(mel.wd_event_date))) AS first_migration_month
        FROM swh.migration_event_log AS mel
        CROSS JOIN Parameters p
        -- Joins provided in your original query
        INNER JOIN swh.scopes_input_type_metrics AS sitm 
            ON mel.source_object_id = sitm.scope_external_id
            AND sitm.wd_event_date IS NOT NULL
            -- Filter Joined Table for Partition Performance
            AND CAST(sitm.wd_event_date AS DATE) >= p.start_date
        JOIN lookup_db.sfdc_customer_account_tenants cat 
            ON mel.source_tenant = cat.tenant_name
        JOIN lookup_db.sfdc_account_details sad 
            ON cat.sf_account_id = sad.sf_account_id
        
        WHERE mel.user_type = 'Customer'
          AND mel.event_type = 'push_migration'
          AND mel.wd_event_date IS NOT NULL
          -- Filter Main Log for Partition Performance
          AND CAST(mel.wd_event_date AS DATE) >= p.start_date
          AND sad.enterprise_size_group IN ('ME', 'LE')
        GROUP BY 1, 2
    ),

    -- 4. MONTHLY COUNTS: Count NEW customers per month
    MonthlyNewArrivals AS (
        SELECT 
            first_migration_month,
            COUNT(DISTINCT CASE WHEN enterprise_size_group = 'ME' THEN tenant_prefix END) AS new_me_customers,
            COUNT(DISTINCT CASE WHEN enterprise_size_group = 'LE' THEN tenant_prefix END) AS new_le_customers
        FROM FirstMigrationEver
        GROUP BY 1
    )

    -- 5. MERGE: Join Calendar to Data to fill gaps with 0
    SELECT 
        c.month_start,
        COALESCE(mna.new_me_customers, 0) AS new_me_customers,
        COALESCE(mna.new_le_customers, 0) AS new_le_customers
    FROM Calendar c
    LEFT JOIN MonthlyNewArrivals mna ON c.month_start = mna.first_migration_month
) AS CleanData
ORDER BY migration_month DESC;

-- ------------------------------------------------------------------------------------------------
-- Enterprise Size Group
WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS period_start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS current_month_start
),
FirstMigrationEver AS (
    SELECT 
        cat.tenant_prefix,
        cat.sf_account_id, -- Used for joining to account details
        MIN(DATE_TRUNC('month', DATE(mel.wd_event_date))) AS first_migration_month
    FROM swh.migration_event_log AS mel
    INNER JOIN swh.scopes_input_type_metrics AS sitm 
        ON mel.source_object_id = sitm.scope_external_id
        AND sitm.wd_event_date IS NOT NULL
    JOIN lookup_db.sfdc_customer_account_tenants cat 
        ON mel.source_tenant = cat.tenant_name
    WHERE mel.user_type = 'Customer'
      AND mel.wd_event_date IS NOT NULL
    GROUP BY 1, 2
),
NewCustomersInPeriod AS (
    SELECT 
        fme.tenant_prefix,
        fme.sf_account_id,
        fme.first_migration_month
    FROM FirstMigrationEver fme
    CROSS JOIN Parameters p
    WHERE fme.first_migration_month >= p.period_start_date
      AND fme.first_migration_month < p.current_month_start
)
SELECT 
    sad.account_name,
    sad.enterprise_size_group,
    ncip.tenant_prefix,
    ncip.first_migration_month,
    sad.billing_id,
    sad.account_type
FROM NewCustomersInPeriod ncip
JOIN lookup_db.sfdc_account_details sad 
    ON ncip.sf_account_id = sad.sf_account_id
ORDER BY ncip.first_migration_month DESC, sad.account_name ASC;

-- ------------------------------------------------------------------------------------------------
-- Customer Utilization: Change Trackers for Migration
WITH ct_usage_status AS (
    SELECT 
        date_trunc('month', ct.time) as event_month,
        ct.user_type,
        sfdc.billing_id as verified_billing_id,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) as has_migration_link
    FROM swh.change_tracker_event_log ct
    -- 1. Link CT to Scope
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        ct.change_tracker_wid = s.input_id
        AND s.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    -- 2. Link Scope to Migration (Push Only)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    -- 3. Verify against SFDC via Tenant Name
    INNER JOIN lookup_db.sfdc_customer_tenants sfdc ON (
        LOWER(ct.tenant) = LOWER(sfdc.tenant_name)
    )
    WHERE ct.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
      AND ct.time >= date_trunc('month', CURRENT_DATE - INTERVAL '6' MONTH)
      AND ct.time < date_trunc('month', CURRENT_DATE)
      AND ct.user_type IN ('Customer', 'Implementer')
      AND sfdc.billing_id IS NOT NULL
    GROUP BY 1, 2, 3
)
-- Final Select: Aggregating into the "Long" format for easy charting
SELECT 
    event_month,
    user_type,
    CASE 
        WHEN has_migration_link = 1 THEN 'Migrated'
        ELSE 'Created Only' 
    END as usage_status,
    COUNT(*) as customer_count
FROM ct_usage_status
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- ------------------------------------------------------------------------------------------------
-- Month-over-Month Customers who have used Change Tracker Migrations in Last 6 Months
WITH Parameters AS (
    -- Centralized configuration
    SELECT 
        6 AS lookback_months,
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS period_start_date,
        -- Calculate the start of the current month to use as a boundary
        DATE_TRUNC('month', CURRENT_DATE) AS current_month_start
),
FirstMigrationEver AS (
    -- Step 1: Find the EARLIEST migration EVER for each customer
    SELECT 
        cat.tenant_prefix,
        MIN(DATE_TRUNC('month', DATE(mel.wd_event_date))) AS first_migration_month
    FROM swh.migration_event_log AS mel
    INNER JOIN swh.scopes_input_type_metrics AS sitm
        ON mel.source_object_id = sitm.scope_external_id
        AND sitm.wd_event_date IS NOT NULL
    JOIN lookup_db.sfdc_customer_account_tenants cat 
        ON mel.source_tenant = cat.tenant_name 
    WHERE mel.user_type = 'Customer'
      AND mel.wd_event_date IS NOT NULL
    GROUP BY 1
),
NewCustomersInPeriod AS (
    -- Step 2: Filter to ONLY customers whose FIRST migration was within the period
    SELECT 
        fme.tenant_prefix,
        fme.first_migration_month
    FROM FirstMigrationEver fme
    CROSS JOIN Parameters p
    WHERE fme.first_migration_month >= p.period_start_date
      -- Exclude migrations that happened in the current month
      AND fme.first_migration_month < p.current_month_start
),
MonthlyNewArrivals AS (
    -- Step 3: Count new customers per month
    SELECT 
        first_migration_month,
        COUNT(DISTINCT tenant_prefix) AS new_customers_this_month
    FROM NewCustomersInPeriod
    GROUP BY 1
),
CumulativeCalculation AS (
    -- Step 4: Calculate cumulative sum
    SELECT 
        first_migration_month,
        SUM(new_customers_this_month) OVER (
            ORDER BY first_migration_month ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_customer_count
    FROM MonthlyNewArrivals
)
-- Step 5: Final Output
SELECT 
    first_migration_month AS migration_month,
    cumulative_customer_count
FROM CumulativeCalculation
ORDER BY migration_month DESC;

-- ------------------------------------------------------------------------------------------------
-- Unique Accounts using Tooling for Migration by Implementer : Initial vs. Phase X Deployments
WITH 
-- 1. GENERATE CALENDAR (Ensures every month exists)
calendar AS (
    SELECT CAST(date_column AS DATE) as month_start
    FROM (
        VALUES 
            (DATE_TRUNC('month', DATE_ADD('month', -1, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -2, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -3, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -4, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -5, CURRENT_DATE))),
            (DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)))
    ) AS t(date_column)
),

-- 2. FILTER POPULATION (Logic from Query 1)
active_population AS (
    SELECT 
        ct.billing_id,
        ct.sf_id AS customer_sf_account_id,
        MIN(DATE_TRUNC('month', CAST(mel.wd_event_date AS DATE))) AS first_active_month
    FROM swh.migration_event_log mel
    INNER JOIN swh.scopes_metrics sm 
        ON mel.source_object_id = sm.scope_external_id
        -- Partition Pruning
        AND sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
    INNER JOIN lookup_db.sfdc_customer_tenants ct 
        ON sm.tenant_name = ct.tenant_name
    WHERE mel.user_type = 'Implementer'       -- Specific filter from Query 1
      AND mel.event_type = 'push_migration'   -- Specific filter from Query 1
      AND mel.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND mel.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
    GROUP BY ct.billing_id, ct.sf_id
),

-- 3. CLASSIFY EACH CUSTOMER (Logic from Query 1)
classified_billing_ids AS (
    SELECT 
        ap.first_active_month,
        ap.billing_id,
        CASE 
            WHEN COUNT(CASE WHEN d.type != 'Initial Deployment' THEN 1 END) > 0 
            THEN 'Phase X Deployment'
            ELSE 'Initial Deployment' 
        END AS deployment_bucket
    FROM active_population ap
    INNER JOIN lookup_db.sfdc_deployments d 
        ON ap.customer_sf_account_id = d.customer_sf_account_id
    GROUP BY ap.first_active_month, ap.billing_id
),

-- 4. AGGREGATE PER MONTH
monthly_aggregates AS (
    SELECT 
        first_active_month AS month_start,
        deployment_bucket,
        COUNT(*) AS new_billing_ids
    FROM classified_billing_ids
    GROUP BY 1, 2
),

-- 5. MERGE WITH CALENDAR (The Fix)
--    We Cross Join the calendar with buckets, then Left Join the data.
--    This fills in "0" for any missing month so the math doesn't break.
full_dataset AS (
    SELECT 
        c.month_start,
        b.deployment_bucket,
        COALESCE(ma.new_billing_ids, 0) AS new_billing_ids
    FROM calendar c
    CROSS JOIN (VALUES ('Initial Deployment'), ('Phase X Deployment')) AS b(deployment_bucket)
    LEFT JOIN monthly_aggregates ma 
        ON c.month_start = ma.month_start 
        AND b.deployment_bucket = ma.deployment_bucket
)

-- 6. FINAL WINDOW CALCULATION
SELECT 
    date_trunc('month', CAST(month_start AS TIMESTAMP)) AS "Month",
    deployment_bucket AS "Deployment Type",
    SUM(new_billing_ids) OVER (
        PARTITION BY deployment_bucket 
        ORDER BY month_start 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "MAX(Total Billing IDs)"
FROM full_dataset
ORDER BY "Month" DESC, "Deployment Type";

-- ------------------------------------------------------------------------------------------------
-- Percentage Penetration for Implementer Users
WITH usage_count AS (
    -- Your simplified usage query
    SELECT 
        COUNT(DISTINCT atp.billing_id) AS active_billing_ids
    FROM swh.migration_event_log mel
    INNER JOIN swh.scopes_metrics sm ON mel.source_object_id = sm.scope_external_id
    INNER JOIN (
        SELECT DISTINCT ct.billing_id, ct.tenant_name
        FROM lookup_db.sfdc_customer_tenants ct
        JOIN lookup_db.sfdc_deployments d ON ct.sf_id = d.customer_sf_account_id
    ) atp ON sm.tenant_name = atp.tenant_name
    WHERE mel.user_type = 'Implementer'
      AND mel.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND mel.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
      AND sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND sm.wd_event_date < format_datetime(DATE_TRUNC('month', CURRENT_DATE), 'yyyy-MM-dd')
),
total_accounts AS (
    -- Your account denominator query
    SELECT
        COUNT(DISTINCT a.sf_account_id) AS total_active_accounts
    FROM
        sfdc_account_details a
    JOIN
        sfdc_deployments d ON a.sf_account_id = d.customer_sf_account_id
    WHERE
        d.overall_status = 'Active'
        AND d.phase != 'Adhoc'
)
SELECT 
    u.active_billing_ids,
    t.total_active_accounts,
    -- Calculate the ratio as a percentage
    (CAST(u.active_billing_ids AS DOUBLE) / t.total_active_accounts) * 100 AS penetration_percentage
FROM usage_count u, total_accounts t;

-- ------------------------------------------------------------------------------------------------
-- Total Number of Active Deployments
SELECT
    COUNT(DISTINCT a.sf_account_id) AS total_active_accounts
FROM
    sfdc_account_details a
JOIN
    sfdc_deployments d ON a.sf_account_id = d.customer_sf_account_id
WHERE
    d.overall_status = 'Active'
    AND d.phase != 'Adhoc';

-- ------------------------------------------------------------------------------------------------
-- Customers with Active Deployments
SELECT DISTINCT
    a.sf_account_id AS Customers_with_Active_Deployments
FROM sfdc_account_details a
INNER JOIN sfdc_deployments d 
    ON a.sf_account_id = d.customer_sf_account_id
WHERE d.overall_status = 'Active'
    AND d.phase NOT IN ('Adhoc', 'Customer Enablement', 'Phase X - Sourcing', 'Customer Led', 'Peakon First', 'Sourcing First', 'Phase X - Peakon', 'Phase X - VNDLY', 'Phase X - Planning')
    AND d.deployment_start_date > DATE '2023-01-01';

-- ------------------------------------------------------------------------------------------------
-- Unique Accounts for Implementer Usage with Enterprise Size Group
SELECT date_trunc('month', CAST("Month" AS TIMESTAMP)) AS "Month",
       "enterprise_size_group",
       sum("Cumulative Unique Billing IDs") AS "SUM(Cumulative Unique Billing IDs)"
FROM
  (WITH active_tenant_pool AS
     (SELECT DISTINCT ct.billing_id,
                      ct.tenant_name
      FROM lookup_db.sfdc_customer_tenants ct
      JOIN lookup_db.sfdc_deployments d ON ct.sf_id = d.customer_sf_account_id),
        first_usage_per_id AS
     (SELECT atp.billing_id,
             sad.enterprise_size_group,
             MIN(DATE_TRUNC('month', CAST(mel.wd_event_date AS DATE))) AS first_active_month
      FROM swh.migration_event_log mel
      INNER JOIN swh.scopes_metrics sm ON mel.source_object_id = sm.scope_external_id
      INNER JOIN active_tenant_pool atp ON sm.tenant_name = atp.tenant_name
      LEFT JOIN lookup_db.sfdc_account_details sad ON atp.billing_id = sad.billing_id
      WHERE mel.user_type = 'Implementer'
        AND mel.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      GROUP BY 1, 2),
        monthly_new_counts AS
     (SELECT first_active_month AS month_start,
             enterprise_size_group,
             COUNT(DISTINCT billing_id) AS new_billing_ids
      FROM first_usage_per_id
      GROUP BY 1, 2) 
   SELECT month_start AS "Month",
          enterprise_size_group,
          SUM(new_billing_ids) OVER (
              PARTITION BY enterprise_size_group
              ORDER BY month_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "Cumulative Unique Billing IDs"
   FROM monthly_new_counts
   WHERE month_start >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
     AND month_start < DATE_TRUNC('month', CURRENT_DATE)
   ORDER BY month_start DESC) AS virtual_table
GROUP BY date_trunc('month', CAST("Month" AS TIMESTAMP)), "enterprise_size_group"
ORDER BY "SUM(Cumulative Unique Billing IDs)" DESC;

-- ------------------------------------------------------------------------------------------------
-- Enterprise Size Group
WITH active_tenant_pool AS (
    SELECT DISTINCT 
        ct.billing_id,
        ct.tenant_name,
        ct.sf_id -- Used to join to account details
    FROM lookup_db.sfdc_customer_tenants ct
    JOIN lookup_db.sfdc_deployments d ON ct.sf_id = d.customer_sf_account_id
),
first_usage_per_id AS (
    SELECT 
        atp.billing_id,
        atp.sf_id,
        MIN(DATE_TRUNC('month', CAST(mel.wd_event_date AS DATE))) AS first_active_month
    FROM swh.migration_event_log mel
    INNER JOIN swh.scopes_metrics sm ON mel.source_object_id = sm.scope_external_id
    INNER JOIN active_tenant_pool atp ON sm.tenant_name = atp.tenant_name
    WHERE mel.user_type = 'Implementer'
      -- Apply the 6-month lookback filter here
      AND mel.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
    GROUP BY 1, 2
)
SELECT 
    sad.account_name,
    sad.enterprise_size_group,
    fupi.billing_id,
    fupi.first_active_month AS activation_month,
    sad.account_type,
    sad.sf_account_id
FROM first_usage_per_id fupi
JOIN lookup_db.sfdc_account_details sad 
    ON fupi.sf_id = sad.sf_account_id
WHERE fupi.first_active_month < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY fupi.first_active_month DESC, sad.account_name ASC;

-- ------------------------------------------------------------------------------------------------
-- Implementer Utilization: Change Trackers for Migration
WITH ct_usage_status AS (
    SELECT 
        date_trunc('month', ct.time) as event_month,
        ct.user_type,
        sfdc.billing_id as verified_billing_id,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) as has_migration_link
    FROM swh.change_tracker_event_log ct
    -- 1. Link CT to Scope
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        ct.change_tracker_wid = s.input_id
        AND s.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    -- 2. Link Scope to Migration (Push Only)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    -- 3. Verify against SFDC via Tenant Name
    INNER JOIN lookup_db.sfdc_customer_tenants sfdc ON (
        LOWER(ct.tenant) = LOWER(sfdc.tenant_name)
    )
    WHERE ct.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
      AND ct.time >= date_trunc('month', CURRENT_DATE - INTERVAL '6' MONTH)
      AND ct.time < date_trunc('month', CURRENT_DATE)
      AND ct.user_type IN ('Customer', 'Implementer')
      AND sfdc.billing_id IS NOT NULL
    GROUP BY 1, 2, 3
)
-- Final Select: Aggregating into the "Long" format for easy charting
SELECT 
    event_month,
    user_type,
    CASE 
        WHEN has_migration_link = 1 THEN 'Migrated'
        ELSE 'Created Only' 
    END as usage_status,
    COUNT(*) as customer_count
FROM ct_usage_status
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- ------------------------------------------------------------------------------------------------
-- Migration Tools Used vs NOT Used by Implementers in Migration Process
WITH combined_data AS (
    -- Change Tracker Stream
    SELECT 
        date_trunc('month', ct.time) as event_month,
        'Change Tracker' as category,
        s.input_id as matched_scope_id
    FROM swh.change_tracker_event_log ct
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        ct.change_tracker_wid = s.input_id 
        AND s.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    WHERE ct.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
      AND ct.time >= date_trunc('month', CURRENT_DATE - INTERVAL '6' MONTH)
      -- Exclude current month (January 2026)
      AND ct.time < date_trunc('month', CURRENT_DATE) 
      AND ct.user_type = 'Implementer'
      AND LOWER(ct.tenant) NOT LIKE '%sales%' 
      AND LOWER(ct.tenant) NOT LIKE '%demo%'
      AND LOWER(ct.tenant) NOT LIKE '%gms%'

    UNION ALL

    -- Tenant Compare Stream
    SELECT 
        date_trunc('month', tc.time) as event_month,
        'Tenant Compare' as category,
        s.input_id as matched_scope_id
    FROM swh.tenant_compare_event_log tc
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        tc.tenant_compare_scope_wid = s.input_id 
        AND s.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
    )
    WHERE tc.wd_event_date >= CAST(CURRENT_DATE - INTERVAL '7' MONTH AS VARCHAR)
      AND tc.time >= date_trunc('month', CURRENT_DATE - INTERVAL '6' MONTH)
      -- Exclude current month (January 2026)
      AND tc.time < date_trunc('month', CURRENT_DATE)
      AND tc.user_type = 'Implementer'
      AND LOWER(tc.tenant) NOT LIKE '%sales%'
      AND LOWER(tc.tenant) NOT LIKE '%demo%'
      AND LOWER(tc.tenant) NOT LIKE '%gms%'
)
SELECT 
    event_month,
    
    -- Combined Columns
    COUNT(*) as total_amt_event_logged,
    COUNT(matched_scope_id) as amt_used_for_migration_process,
    COUNT(CASE WHEN matched_scope_id IS NULL THEN 1 END) as amt_not_used_for_migration_process,
    
    -- Change Tracker Columns
    COUNT(CASE WHEN category = 'Change Tracker' THEN 1 END) as ct_total,
    COUNT(CASE WHEN category = 'Change Tracker' AND matched_scope_id IS NOT NULL THEN 1 END) as ct_used,
    COUNT(CASE WHEN category = 'Change Tracker' AND matched_scope_id IS NULL THEN 1 END) as ct_unused,
    
    -- Tenant Compare Columns
    COUNT(CASE WHEN category = 'Tenant Compare' THEN 1 END) as tc_total,
    COUNT(CASE WHEN category = 'Tenant Compare' AND matched_scope_id IS NOT NULL THEN 1 END) as tc_used,
    COUNT(CASE WHEN category = 'Tenant Compare' AND matched_scope_id IS NULL THEN 1 END) as tc_unused
FROM combined_data
GROUP BY 1
ORDER BY 1 DESC;