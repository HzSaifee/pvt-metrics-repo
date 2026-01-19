WITH Parameters AS(
        SELECT 6 AS lookback_months,
            DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS period_start_date,
            DATE_TRUNC('month', CURRENT_DATE) AS current_month_start
    ),
    FirstMigrationEver AS(
        SELECT cat.tenant_prefix,
            sad.enterprise_size_group,
            MIN(DATE_TRUNC('month', DATE(mel.wd_event_date))) AS first_migration_month
        FROM swh.migration_event_log AS mel
        CROSS JOIN Parameters p
        INNER JOIN swh.scopes_input_type_metrics AS sitm 
            ON mel.source_object_id = sitm.scope_external_id
            AND sitm.wd_event_date IS NOT NULL
        JOIN lookup_db.sfdc_customer_account_tenants cat 
            ON mel.source_tenant = cat.tenant_name
        JOIN lookup_db.sfdc_account_details sad 
            ON cat.sf_account_id = sad.sf_account_id
        WHERE mel.user_type = 'Customer'
            AND mel.wd_event_date IS NOT NULL
            AND CAST(mel.wd_event_date AS DATE) >= p.period_start_date
            AND sad.enterprise_size_group IN ('ME','LE')
        GROUP BY 1, 2
    ),
    NewCustomersInPeriod AS(
        SELECT fme.tenant_prefix,
            fme.enterprise_size_group,
            fme.first_migration_month
        FROM FirstMigrationEver fme
        CROSS JOIN Parameters p
        WHERE fme.first_migration_month >= p.period_start_date
            AND fme.first_migration_month < p.current_month_start
    ),
    MonthlyNewArrivals AS(
        SELECT first_migration_month,
            COUNT(DISTINCT CASE
                    WHEN enterprise_size_group = 'ME' THEN tenant_prefix
                END) AS new_me_customers,
            COUNT(DISTINCT CASE
                    WHEN enterprise_size_group = 'LE' THEN tenant_prefix
                END) AS new_le_customers
        FROM NewCustomersInPeriod
        GROUP BY 1
    ),
    CumulativeCalculation AS(
        SELECT first_migration_month,
                SUM(new_me_customers) OVER (
                                            ORDER BY first_migration_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_me_count,
                SUM(new_le_customers) OVER (
                                            ORDER BY first_migration_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_le_count
            FROM MonthlyNewArrivals
    )
SELECT first_migration_month AS migration_month,
        cumulative_me_count,
        cumulative_le_count
FROM CumulativeCalculation
ORDER BY migration_month DESC