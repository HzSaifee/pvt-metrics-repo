-- =============================================================================
-- TAM-SAM Penetration Datasource - OVERALL VIEW
-- =============================================================================
-- Purpose: Combined datasource for penetration percentage calculations
--          Includes Activity data (numerator) + Denominators in single datasource
--
-- Row Types:
--   - ACTIVITY: Tool activity rows (for penetration numerator)
--   - ACTIVE_CUSTOMER: Active customer denominator rows
--   - ACTIVE_DEPLOYMENT: Customers with active deployments denominator rows
--
-- Penetration Calculation (in Tableau):
--   Penetration % = COUNTD(sf_account_id WHERE row_type='ACTIVITY') 
--                   / COUNTD(sf_account_id WHERE row_type='ACTIVE_CUSTOMER') * 100
--
-- Key Columns for Tableau:
--   - row_type: Filter to select numerator vs denominator
--   - customer_status: 'Created Only' or 'Migrated' (filter for numerator)
--   - deployment_type: 'Initial Deployment' or 'Subsequent Deployment'
--   - deployment_overall_status: 'Active' or 'Completed'
--
-- =============================================================================

WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_TRUNC('month', CURRENT_DATE)
            ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
        END AS end_date,
        -- Shifted window for non-Launch Express Initial Deployments by 6 months based on Migration Recipe Usage from Initial Deployment Start Date
        DATE_TRUNC('month', DATE_ADD('month', -(6 + 6), CURRENT_DATE)) AS initial_start_date_ac,
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
            ELSE DATE_ADD('month', -6, DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE)))
        END AS initial_end_date_ac
),

-- =============================================================================
-- PART A: ACTIVITY DATA (Numerator for Penetration)
-- Adapted from activity_datasource_overall.sql
-- =============================================================================

scopes_input_filtered AS (
    SELECT 
        input_id,
        scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

migration_filtered AS (
    SELECT 
        event_id,
        source_object_id
    FROM dw.swh.migration_event_log
    CROSS JOIN Parameters p
    WHERE event_type = 'push_migration'
      AND wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

account_deployments AS (
    -- Subsequent + Launch Express Initial: standard 6-month window, Active + Complete
    SELECT 
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        overall_status AS deployment_overall_status,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    CROSS JOIN Parameters p
    WHERE overall_status IN ('Active', 'Complete')
        AND (type != 'Initial Deployment' OR phase = 'Launch Express')
        AND deployment_start_date >= p.start_date
        AND deployment_start_date <= p.end_date
    UNION ALL
    -- Non-Launch Express Initial Deployments: shifted window, Active + Complete
    SELECT 
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        overall_status AS deployment_overall_status,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    CROSS JOIN Parameters p
    WHERE overall_status IN ('Active', 'Complete')
        AND type = 'Initial Deployment'
        AND (phase IS NULL OR phase != 'Launch Express')
        AND deployment_start_date >= p.initial_start_date_ac
        AND deployment_start_date <= p.initial_end_date_ac
),

account_deployment_combos AS (
    SELECT DISTINCT
        customer_sf_account_id,
        deployment_product_area,
        deployment_partner,
        deployment_type,
        deployment_phase,
        deployment_overall_status,
        MAX(deployment_start_date) AS latest_deployment_start
    FROM account_deployments
    GROUP BY 1, 2, 3, 4, 5, 6
),

change_tracker_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(ct.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(ct.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(ct.time AS DATE))))
        END AS biweekly_period,
        ct.user_type,
        ct.tenant,
        ct.change_tracker_wid,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON ct.change_tracker_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE 
        ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND ct.time >= p.start_date
        AND ct.time < p.end_date
        AND ct.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

tenant_compare_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(tc.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tc.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tc.time AS DATE))))
        END AS biweekly_period,
        tc.user_type,
        tc.tenant,
        tc.tenant_compare_scope_wid,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN scopes_input_filtered s ON tc.tenant_compare_scope_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE 
        tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND tc.time >= p.start_date
        AND tc.time < p.end_date
        AND tc.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

scopes_with_input AS (
    SELECT DISTINCT scope_external_id
    FROM scopes_input_filtered
),

manual_scope_events AS (
    SELECT 
        CASE 
            WHEN DAY(CAST(sm.time AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(sm.time AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(sm.time AS DATE))))
        END AS biweekly_period,
        sm.user_type,
        sm.tenant_name AS tenant,
        sm.scope_external_id,
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM dw.swh.scopes_metrics sm
    CROSS JOIN Parameters p
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered m ON sm.scope_external_id = m.source_object_id
    WHERE 
        sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        AND sm.time >= p.start_date
        AND sm.time < p.end_date
        AND sm.user_type IN ('Customer', 'Implementer')
        AND swi.scope_external_id IS NULL
    GROUP BY 1, 2, 3, 4
),

combined_tool_events AS (
    SELECT biweekly_period, user_type, tenant, 'Change Tracker' AS tool_type, has_migration_link
    FROM change_tracker_events
    UNION ALL
    SELECT biweekly_period, user_type, tenant, 'Tenant Compare' AS tool_type, has_migration_link
    FROM tenant_compare_events
    UNION ALL
    SELECT biweekly_period, user_type, tenant, 'Adhoc Scope' AS tool_type, has_migration_link
    FROM manual_scope_events
),

enriched_activity AS (
    SELECT 
        cte.biweekly_period,
        cte.user_type,
        cte.tool_type,
        cte.has_migration_link,
        sfdc.billing_id,
        sfdc.tenant_type AS tenant_env_type,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(adc.deployment_product_area, 'No Active Deployment') AS deployment_product_area,
        COALESCE(adc.deployment_partner, 'No Active Deployment') AS deployment_partner,
        COALESCE(adc.deployment_type, 'No Active Deployment') AS deployment_type,
        COALESCE(adc.deployment_phase, 'No Active Deployment') AS deployment_phase,
        COALESCE(adc.deployment_overall_status, 'No Active Deployment') AS deployment_overall_status,
        adc.latest_deployment_start
    FROM combined_tool_events cte
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sfdc 
        ON LOWER(cte.tenant) = LOWER(sfdc.tenant_name)
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON sfdc.sf_id = sad.sf_account_id
    LEFT JOIN account_deployment_combos adc
        ON sfdc.sf_id = adc.customer_sf_account_id
    WHERE sfdc.billing_id IS NOT NULL
),

first_created_overall AS (
    SELECT 
        billing_id,
        user_type,
        MIN(biweekly_period) AS first_created_period
    FROM enriched_activity
    GROUP BY 1, 2
),

first_migrated_overall AS (
    SELECT 
        billing_id,
        user_type,
        MIN(biweekly_period) AS first_migrated_period
    FROM enriched_activity
    WHERE has_migration_link = 1
    GROUP BY 1, 2
),

customer_event_date AS (
    SELECT 
        fc.billing_id,
        fc.user_type,
        fc.first_created_period,
        fm.first_migrated_period,
        COALESCE(fm.first_migrated_period, fc.first_created_period) AS event_period,
        CASE 
            WHEN fm.first_migrated_period IS NOT NULL THEN 'Migrated'
            ELSE 'Created Only'
        END AS customer_status
    FROM first_created_overall fc
    LEFT JOIN first_migrated_overall fm 
        ON fc.billing_id = fm.billing_id 
        AND fc.user_type = fm.user_type
),

customer_dimensions AS (
    SELECT DISTINCT
        e.billing_id,
        e.user_type,
        e.sf_account_id,
        e.account_name,
        e.tenant_env_type,
        e.enterprise_size_group,
        e.segment,
        e.industry,
        e.super_industry,
        e.segment_size_l1,
        e.deployment_product_area,
        e.deployment_partner,
        e.deployment_type,
        e.deployment_phase,
        e.deployment_overall_status,
        e.latest_deployment_start
    FROM enriched_activity e
),

activity_final AS (
    SELECT 
        ced.event_period AS biweekly_period,
        ced.billing_id,
        ced.user_type,
        ced.customer_status,
        ced.first_created_period,
        ced.first_migrated_period,
        cd.sf_account_id,
        cd.account_name,
        cd.tenant_env_type,
        cd.enterprise_size_group,
        cd.segment,
        cd.industry,
        cd.super_industry,
        cd.segment_size_l1,
        cd.deployment_product_area,
        cd.deployment_partner,
        cd.deployment_type,
        cd.deployment_phase,
        cd.deployment_overall_status,
        cd.latest_deployment_start
    FROM customer_event_date ced
    INNER JOIN customer_dimensions cd 
        ON ced.billing_id = cd.billing_id 
        AND ced.user_type = cd.user_type
),

activity_output AS (
    SELECT 
        'ACTIVITY' AS row_type,
        biweekly_period,
        billing_id,
        sf_account_id,
        user_type,
        customer_status,
        first_created_period,
        first_migrated_period,
        account_name,
        tenant_env_type,
        enterprise_size_group,
        segment,
        industry,
        super_industry,
        segment_size_l1,
        deployment_product_area,
        deployment_partner,
        deployment_type,
        deployment_phase,
        deployment_overall_status
    FROM activity_final
),

-- =============================================================================
-- PART B: ACTIVE CUSTOMERS DENOMINATOR
-- =============================================================================

-- Deployments for Active Customers (no date filter, no overall_status filter, only phase/product_area exclusions)
ac_deployment_combos AS (
    SELECT DISTINCT
        d.customer_sf_account_id,
        COALESCE(NULLIF(d.product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(d.type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        COALESCE(d.overall_status, 'Unknown') AS deployment_overall_status,
        d.deployment_start_date
    FROM dw.lookup_db.sfdc_deployments d
),

-- Base Active Customers with deployment join
active_customers_base AS (
    SELECT 
        sct.billing_id,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        sct.tenant_type AS tenant_env_type,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(acd.deployment_product_area, 'No Active Deployment') AS deployment_product_area,
        COALESCE(acd.deployment_partner, 'No Active Deployment') AS deployment_partner,
        COALESCE(acd.deployment_type, 'No Active Deployment') AS deployment_type,
        COALESCE(acd.deployment_phase, 'No Active Deployment') AS deployment_phase,
        COALESCE(acd.deployment_overall_status, 'No Active Deployment') AS deployment_overall_status,
        acd.deployment_start_date
    FROM dw.lookup_db.sfdc_customer_tenants sct
    CROSS JOIN Parameters p
    INNER JOIN dw.lookup_db.sfdc_account_details sad 
        ON sct.sf_id = sad.sf_account_id
    INNER JOIN dw.lookup_db.sfdc_customer_account_tenants scat 
        ON sct.sf_id = scat.sf_account_id
        AND sct.tenant_name = scat.tenant_name
    LEFT JOIN ac_deployment_combos acd
        ON sad.sf_account_id = acd.customer_sf_account_id
    WHERE sct.tenant_type = 'Production'
      AND sct.status = 'Active'
      AND sct.tenant_start_date <= p.end_date
      AND (sct.tenant_expire_date IS NULL OR sct.tenant_expire_date >= p.end_date)
      AND scat.tenant_prefix IS NOT NULL
      AND sad.assumed_enterprise_go_live_date IS NOT NULL
      AND sad.segment NOT IN (
        'CSD EMEA',
        'Specialized',
        'US Federal'
      )
      AND (
          acd.customer_sf_account_id IS NULL
          OR (
              acd.deployment_phase NOT IN (
                  'Adhoc',
                  'Peakon First',
                  'Phase - X - Planning',
                  'Phase X - Peakon',
                  'Phase X - Planning',
                  'Phase X - Sourcing',
                  'Phase X - VNDLY',
                  'Planning First',
                  'Sourcing First',
                  'VNDLY First'
              )
              AND COALESCE(acd.deployment_product_area, '') NOT IN (
                  'Adaptive Planning',
                  'HiredScore',
                  'Planning',
                  'VNDLY',
                  'Workday HiredScore',
                  'Workday Peakon Employee Voice',
                  'Workday Success Plans',
                  'Workday VNDLY'
              )
              AND acd.deployment_overall_status IN ('Active', 'Complete')
          )
      )
),

active_customers AS (
    SELECT 
        'ACTIVE_CUSTOMER' AS row_type,
        p.end_date AS biweekly_period,
        acb.billing_id,
        acb.sf_account_id,
        'Customer' AS user_type,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS DATE) AS first_created_period,
        CAST(NULL AS DATE) AS first_migrated_period,
        acb.account_name,
        acb.tenant_env_type,
        acb.enterprise_size_group,
        acb.segment,
        acb.industry,
        acb.super_industry,
        acb.segment_size_l1,
        acb.deployment_product_area,
        acb.deployment_partner,
        acb.deployment_type,
        acb.deployment_phase,
        acb.deployment_overall_status
    FROM active_customers_base acb
    CROSS JOIN Parameters p
    UNION ALL
    SELECT 
        'ACTIVE_CUSTOMER' AS row_type,
        p.end_date AS biweekly_period,
        acb.billing_id,
        acb.sf_account_id,
        'Implementer' AS user_type,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS DATE) AS first_created_period,
        CAST(NULL AS DATE) AS first_migrated_period,
        acb.account_name,
        acb.tenant_env_type,
        acb.enterprise_size_group,
        acb.segment,
        acb.industry,
        acb.super_industry,
        acb.segment_size_l1,
        acb.deployment_product_area,
        acb.deployment_partner,
        acb.deployment_type,
        acb.deployment_phase,
        acb.deployment_overall_status
    FROM active_customers_base acb
    CROSS JOIN Parameters p
),

-- =============================================================================
-- PART C: CUSTOMERS WITH ACTIVE DEPLOYMENTS DENOMINATOR
-- UNION: Subsequent + LE Initial (standard window, Active only)
-- vs Non-LE Initial (shifted window, Active + Complete)
-- =============================================================================

active_deployments_base AS (
    -- Subsequent + Launch Express Initial: standard 6-month window, Active only
    SELECT 
        d.customer_sf_account_id,
        COALESCE(sad.billing_id, 'Unknown') AS billing_id,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(NULLIF(d.product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(d.type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        d.overall_status AS deployment_overall_status,
        d.deployment_start_date
    FROM dw.lookup_db.sfdc_deployments d
    CROSS JOIN Parameters p
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status = 'Active'
      AND (d.type != 'Initial Deployment' OR d.phase = 'Launch Express')
      AND d.phase NOT IN (
          'Adhoc',
          'Customer Enablement',
          'Customer Led',
          'Peakon First',
          'Phase - X - Planning',
          'Phase X - Peakon',
          'Phase X - Planning',
          'Phase X - Sourcing',
          'Phase X - VNDLY',
          'Planning First',
          'Sourcing First',
          'VNDLY First'
      )
      AND COALESCE(d.product_area, '') NOT IN (
          'Adaptive Planning',
          'HiredScore',
          'Planning',
          'VNDLY',
          'Workday HiredScore',
          'Workday Peakon Employee Voice',
          'Workday Success Plans',
          'Workday VNDLY'
      )
      AND sad.segment NOT IN (
        'CSD EMEA',
        'Specialized',
        'US Federal'
      )
      AND d.deployment_start_date >= p.start_date
      AND d.deployment_start_date <= p.end_date
    UNION ALL
    -- Non-Launch Express Initial Deployments: shifted window, Active + Complete
    SELECT 
        d.customer_sf_account_id,
        COALESCE(sad.billing_id, 'Unknown') AS billing_id,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(NULLIF(d.product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(d.priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(d.type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        d.overall_status AS deployment_overall_status,
        d.deployment_start_date
    FROM dw.lookup_db.sfdc_deployments d
    CROSS JOIN Parameters p
    LEFT JOIN dw.lookup_db.sfdc_account_details sad 
        ON d.customer_sf_account_id = sad.sf_account_id
    WHERE d.overall_status IN ('Active', 'Complete')
      AND d.type = 'Initial Deployment'
      AND (d.phase IS NULL OR d.phase != 'Launch Express')
      AND d.phase NOT IN (
          'Adhoc',
          'Customer Enablement',
          'Customer Led',
          'Peakon First',
          'Phase - X - Planning',
          'Phase X - Peakon',
          'Phase X - Planning',
          'Phase X - Sourcing',
          'Phase X - VNDLY',
          'Planning First',
          'Sourcing First',
          'VNDLY First'
      )
      AND COALESCE(d.product_area, '') NOT IN (
          'Adaptive Planning',
          'HiredScore',
          'Planning',
          'VNDLY',
          'Workday HiredScore',
          'Workday Peakon Employee Voice',
          'Workday Success Plans',
          'Workday VNDLY'
      )
      AND sad.segment NOT IN (
        'CSD EMEA',
        'Specialized',
        'US Federal'
      )
      AND d.deployment_start_date >= p.initial_start_date_ac
      AND d.deployment_start_date <= p.initial_end_date_ac
),

active_deployments AS (
    SELECT 
        'ACTIVE_DEPLOYMENT' AS row_type,
        p.end_date AS biweekly_period,
        adb.billing_id,
        adb.sf_account_id,
        'Customer' AS user_type,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS DATE) AS first_created_period,
        CAST(NULL AS DATE) AS first_migrated_period,
        adb.account_name,
        'N/A - Deployment' AS tenant_env_type,
        adb.enterprise_size_group,
        adb.segment,
        adb.industry,
        adb.super_industry,
        adb.segment_size_l1,
        adb.deployment_product_area,
        adb.deployment_partner,
        adb.deployment_type,
        adb.deployment_phase,
        adb.deployment_overall_status
    FROM active_deployments_base adb
    CROSS JOIN Parameters p
    UNION ALL
    SELECT 
        'ACTIVE_DEPLOYMENT' AS row_type,
        p.end_date AS biweekly_period,
        adb.billing_id,
        adb.sf_account_id,
        'Implementer' AS user_type,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS DATE) AS first_created_period,
        CAST(NULL AS DATE) AS first_migrated_period,
        adb.account_name,
        'N/A - Deployment' AS tenant_env_type,
        adb.enterprise_size_group,
        adb.segment,
        adb.industry,
        adb.super_industry,
        adb.segment_size_l1,
        adb.deployment_product_area,
        adb.deployment_partner,
        adb.deployment_type,
        adb.deployment_phase,
        adb.deployment_overall_status
    FROM active_deployments_base adb
    CROSS JOIN Parameters p
)

-- =============================================================================
-- FINAL OUTPUT: UNION ALL THREE PARTS
-- =============================================================================

SELECT * FROM activity_output
UNION ALL
SELECT * FROM active_customers
UNION ALL
SELECT * FROM active_deployments
ORDER BY row_type, user_type, sf_account_id
