-- =============================================================================
-- TAM Penetration CTEs — extracted from penetration_datasource_per_tool.sql
-- Date simplification applied: biweekly boundaries replaced with CURRENT_DATE
-- =============================================================================

-- =============================================================================
-- SHARED: Unified Deployment Logic (date-windowed, with exclusions)
-- =============================================================================

unified_deployments AS (
    -- Subsequent + Launch Express Initial: standard 6-month window, Active only
    SELECT
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        overall_status AS deployment_overall_status,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    WHERE overall_status = 'Active'
        AND (type != 'Initial Deployment' OR phase = 'Launch Express')
        AND COALESCE(phase, '') NOT IN (
            'Adhoc', 'Peakon First', 'Phase - X - Planning', 'Phase X - Peakon',
            'Phase X - Planning', 'Phase X - Sourcing', 'Phase X - VNDLY',
            'Planning First', 'Sourcing First', 'VNDLY First'
        )
        AND COALESCE(product_area, '') NOT IN (
            'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
            'Workday HiredScore', 'Workday Peakon Employee Voice',
            'Workday Success Plans', 'Workday VNDLY'
        )
        AND deployment_start_date >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
        AND deployment_start_date <= CURRENT_DATE
    UNION ALL
    -- Non-Launch Express Initial: shifted window, Active + Complete
    SELECT
        customer_sf_account_id,
        COALESCE(NULLIF(product_area, ''), 'Unknown') AS deployment_product_area,
        COALESCE(NULLIF(priming_partner_name, ''), 'Unknown') AS deployment_partner,
        COALESCE(NULLIF(type, ''), 'Unknown') AS deployment_type,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS deployment_phase,
        overall_status AS deployment_overall_status,
        deployment_start_date
    FROM dw.lookup_db.sfdc_deployments
    WHERE overall_status IN ('Active', 'Complete')
        AND type = 'Initial Deployment'
        AND (phase IS NULL OR phase != 'Launch Express')
        AND COALESCE(phase, '') NOT IN (
            'Adhoc', 'Peakon First', 'Phase - X - Planning', 'Phase X - Peakon',
            'Phase X - Planning', 'Phase X - Sourcing', 'Phase X - VNDLY',
            'Planning First', 'Sourcing First', 'VNDLY First'
        )
        AND COALESCE(product_area, '') NOT IN (
            'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
            'Workday HiredScore', 'Workday Peakon Employee Voice',
            'Workday Success Plans', 'Workday VNDLY'
        )
        AND deployment_start_date >= DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE))
        AND deployment_start_date <= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
),

unified_deployment_combos AS (
    SELECT DISTINCT
        customer_sf_account_id,
        deployment_product_area,
        deployment_partner,
        deployment_type,
        deployment_phase,
        deployment_overall_status,
        MAX(deployment_start_date) AS latest_deployment_start
    FROM unified_deployments
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- =============================================================================
-- PART A: ACTIVITY DATA PER TOOL (Numerator)
-- =============================================================================

scopes_input_filtered AS (
    SELECT input_id, scope_external_id
    FROM dw.swh.scopes_input_type_metrics
    WHERE wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(CURRENT_DATE, 'yyyy-MM-dd')
),

migration_filtered AS (
    SELECT event_id, source_object_id
    FROM dw.swh.migration_event_log
    WHERE event_type = 'push_migration'
      AND wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(CURRENT_DATE, 'yyyy-MM-dd')
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
    LEFT JOIN scopes_input_filtered s ON ct.change_tracker_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE
        ct.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND ct.wd_event_date < format_datetime(CURRENT_DATE, 'yyyy-MM-dd')
        AND ct.time >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
        AND ct.time < CURRENT_DATE
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
    LEFT JOIN scopes_input_filtered s ON tc.tenant_compare_scope_wid = s.input_id
    LEFT JOIN migration_filtered m ON s.scope_external_id = m.source_object_id
    WHERE
        tc.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND tc.wd_event_date < format_datetime(CURRENT_DATE, 'yyyy-MM-dd')
        AND tc.time >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
        AND tc.time < CURRENT_DATE
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
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    LEFT JOIN migration_filtered m ON sm.scope_external_id = m.source_object_id
    WHERE
        sm.wd_event_date >= format_datetime(DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)), 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(CURRENT_DATE, 'yyyy-MM-dd')
        AND sm.time >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
        AND sm.time < CURRENT_DATE
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

enriched_activity_ct_tc_as AS (
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
        COALESCE(udc.deployment_product_area, 'No Deployment') AS deployment_product_area,
        COALESCE(udc.deployment_partner, 'No Deployment') AS deployment_partner,
        COALESCE(udc.deployment_type, 'No Deployment') AS deployment_type,
        COALESCE(udc.deployment_phase, 'No Deployment') AS deployment_phase,
        COALESCE(udc.deployment_overall_status, 'No Deployment') AS deployment_overall_status,
        udc.latest_deployment_start,
        CAST(NULL AS VARCHAR) AS build_status
    FROM combined_tool_events cte
    INNER JOIN dw.lookup_db.sfdc_customer_tenants sfdc
        ON LOWER(cte.tenant) = LOWER(sfdc.tenant_name)
    LEFT JOIN dw.lookup_db.sfdc_account_details sad
        ON sfdc.sf_id = sad.sf_account_id
    LEFT JOIN unified_deployment_combos udc
        ON sfdc.sf_id = udc.customer_sf_account_id
    WHERE sfdc.billing_id IS NOT NULL
),

fr_mr_events AS (
    SELECT
        CASE
            WHEN DAY(CAST(tb.wd_event_date AS DATE)) <= 15
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(tb.wd_event_date AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(tb.wd_event_date AS DATE))))
        END AS biweekly_period,
        'Implementer' AS user_type,
        CASE
            WHEN tb.build_type = 'Foundation Tenant Build' THEN 'Foundation Recipe'
            ELSE 'Migration Recipe'
        END AS tool_type,
        1 AS has_migration_link,
        sad.billing_id,
        'N/A - Build' AS tenant_env_type,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(udc.deployment_product_area, 'No Deployment') AS deployment_product_area,
        COALESCE(udc.deployment_partner, 'No Deployment') AS deployment_partner,
        COALESCE(udc.deployment_type, 'No Deployment') AS deployment_type,
        COALESCE(udc.deployment_phase, 'No Deployment') AS deployment_phase,
        COALESCE(udc.deployment_overall_status, 'No Deployment') AS deployment_overall_status,
        udc.latest_deployment_start,
        tb.build_status
    FROM dw.swh.tenant_build tb
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON tb.customer_billing_id = sad.billing_id
    LEFT JOIN unified_deployment_combos udc
        ON sad.sf_account_id = udc.customer_sf_account_id
    WHERE tb.build_type IN ('Foundation Tenant Build', 'Migration Recipe')
      AND CAST(tb.wd_event_date AS DATE) >= DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE))
      AND CAST(tb.wd_event_date AS DATE) < CURRENT_DATE
      AND sad.sf_account_id IS NOT NULL
),

enriched_activity AS (
    SELECT * FROM enriched_activity_ct_tc_as
    UNION ALL
    SELECT * FROM fr_mr_events
),

first_created_per_tool AS (
    SELECT billing_id, user_type, tool_type, MIN(biweekly_period) AS first_created_period
    FROM enriched_activity
    GROUP BY 1, 2, 3
),

first_migrated_per_tool AS (
    SELECT billing_id, user_type, tool_type, MIN(biweekly_period) AS first_migrated_period
    FROM enriched_activity
    WHERE has_migration_link = 1
    GROUP BY 1, 2, 3
),

build_status_per_tool AS (
    SELECT
        billing_id,
        user_type,
        tool_type,
        CASE
            WHEN MAX(CASE WHEN build_status = 'Completed' THEN 1 ELSE 0 END) = 1 THEN 'Completed'
            WHEN MAX(CASE WHEN build_status IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN 'Not Completed'
            ELSE NULL
        END AS build_status
    FROM enriched_activity
    GROUP BY 1, 2, 3
),

customer_tool_event_date AS (
    SELECT
        fc.billing_id,
        fc.user_type,
        fc.tool_type,
        fc.first_created_period,
        fm.first_migrated_period,
        bs.build_status,
        COALESCE(fm.first_migrated_period, fc.first_created_period) AS event_period,
        CASE
            WHEN fm.first_migrated_period IS NOT NULL THEN 'Migrated'
            ELSE 'Created Only'
        END AS customer_status
    FROM first_created_per_tool fc
    LEFT JOIN first_migrated_per_tool fm
        ON fc.billing_id = fm.billing_id
        AND fc.user_type = fm.user_type
        AND fc.tool_type = fm.tool_type
    LEFT JOIN build_status_per_tool bs
        ON fc.billing_id = bs.billing_id
        AND fc.user_type = bs.user_type
        AND fc.tool_type = bs.tool_type
),

customer_tool_dimensions AS (
    SELECT DISTINCT
        e.billing_id, e.user_type, e.tool_type, e.sf_account_id,
        e.account_name, e.tenant_env_type, e.enterprise_size_group,
        e.segment, e.industry, e.super_industry, e.segment_size_l1,
        e.deployment_product_area, e.deployment_partner, e.deployment_type,
        e.deployment_phase, e.deployment_overall_status, e.latest_deployment_start
    FROM enriched_activity e
),

activity_final AS (
    SELECT
        cted.event_period AS biweekly_period,
        cted.billing_id,
        cted.user_type,
        cted.tool_type,
        cted.customer_status,
        cted.build_status,
        cted.first_created_period,
        cted.first_migrated_period,
        ctd.sf_account_id,
        ctd.account_name,
        ctd.tenant_env_type,
        ctd.enterprise_size_group,
        ctd.segment,
        ctd.industry,
        ctd.super_industry,
        ctd.segment_size_l1,
        ctd.deployment_product_area,
        ctd.deployment_partner,
        ctd.deployment_type,
        ctd.deployment_phase,
        ctd.deployment_overall_status
    FROM customer_tool_event_date cted
    INNER JOIN customer_tool_dimensions ctd
        ON cted.billing_id = ctd.billing_id
        AND cted.user_type = ctd.user_type
        AND cted.tool_type = ctd.tool_type
),

-- =============================================================================
-- PART B: ACTIVE CUSTOMERS DENOMINATOR
-- =============================================================================

ac_qualification_deployments AS (
    SELECT DISTINCT
        d.customer_sf_account_id,
        COALESCE(NULLIF(d.phase, ''), 'Unknown') AS deployment_phase,
        COALESCE(NULLIF(d.product_area, ''), '') AS deployment_product_area,
        COALESCE(d.overall_status, 'Unknown') AS deployment_overall_status
    FROM dw.lookup_db.sfdc_deployments d
),

active_customer_accounts AS (
    SELECT DISTINCT sad.sf_account_id
    FROM dw.lookup_db.sfdc_customer_tenants sct
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON sct.sf_id = sad.sf_account_id
    INNER JOIN dw.lookup_db.sfdc_customer_account_tenants scat
        ON sct.sf_id = scat.sf_account_id
        AND sct.tenant_name = scat.tenant_name
    LEFT JOIN ac_qualification_deployments aqd
        ON sad.sf_account_id = aqd.customer_sf_account_id
    WHERE sct.tenant_type = 'Production'
      AND sct.status = 'Active'
      AND sct.tenant_start_date <= CURRENT_DATE
      AND (sct.tenant_expire_date IS NULL OR sct.tenant_expire_date >= CURRENT_DATE)
      AND scat.tenant_prefix IS NOT NULL
      AND sad.assumed_enterprise_go_live_date IS NOT NULL
      AND sad.segment NOT IN ('CSD EMEA', 'Specialized', 'US Federal')
      AND (
          aqd.customer_sf_account_id IS NULL
          OR (
              aqd.deployment_phase NOT IN (
                  'Adhoc', 'Peakon First', 'Phase - X - Planning', 'Phase X - Peakon',
                  'Phase X - Planning', 'Phase X - Sourcing', 'Phase X - VNDLY',
                  'Planning First', 'Sourcing First', 'VNDLY First'
              )
              AND aqd.deployment_product_area NOT IN (
                  'Adaptive Planning', 'HiredScore', 'Planning', 'VNDLY',
                  'Workday HiredScore', 'Workday Peakon Employee Voice',
                  'Workday Success Plans', 'Workday VNDLY'
              )
              AND aqd.deployment_overall_status IN ('Active', 'Complete')
          )
      )
),

active_customers_base AS (
    SELECT DISTINCT
        sct.billing_id,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        sct.tenant_type AS tenant_env_type,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        COALESCE(udc.deployment_product_area, 'No Deployment') AS deployment_product_area,
        COALESCE(udc.deployment_partner, 'No Deployment') AS deployment_partner,
        COALESCE(udc.deployment_type, 'No Deployment') AS deployment_type,
        COALESCE(udc.deployment_phase, 'No Deployment') AS deployment_phase,
        COALESCE(udc.deployment_overall_status, 'No Deployment') AS deployment_overall_status
    FROM dw.lookup_db.sfdc_customer_tenants sct
    INNER JOIN dw.lookup_db.sfdc_account_details sad
        ON sct.sf_id = sad.sf_account_id
    INNER JOIN active_customer_accounts aca
        ON sad.sf_account_id = aca.sf_account_id
    LEFT JOIN unified_deployment_combos udc
        ON sad.sf_account_id = udc.customer_sf_account_id
    WHERE sct.tenant_type = 'Production'
),

-- =============================================================================
-- PART C: CUSTOMERS WITH ACTIVE DEPLOYMENTS DENOMINATOR
-- =============================================================================

active_deployments_base AS (
    SELECT
        ud.customer_sf_account_id,
        COALESCE(sad.billing_id, 'Unknown') AS billing_id,
        sad.sf_account_id,
        COALESCE(sad.account_name, 'Unknown') AS account_name,
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.super_industry, 'Unknown') AS super_industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1,
        ud.deployment_product_area,
        ud.deployment_partner,
        ud.deployment_type,
        ud.deployment_phase,
        ud.deployment_overall_status
    FROM unified_deployments ud
    LEFT JOIN dw.lookup_db.sfdc_account_details sad
        ON ud.customer_sf_account_id = sad.sf_account_id
    WHERE COALESCE(sad.segment, '') NOT IN ('CSD EMEA', 'Specialized', 'US Federal')
),

-- =============================================================================
-- SINGLE-PASS OUTPUT: tagged_rows + window functions for boolean flags
-- =============================================================================

tagged_rows AS (
    -- Section A: Activity rows
    SELECT
        'A' AS src,
        af.biweekly_period,
        af.billing_id,
        af.sf_account_id,
        af.user_type,
        af.tool_type,
        CASE
            WHEN af.tool_type IN ('Change Tracker', 'Tenant Compare', 'Adhoc Scope') THEN 'CT_TC_AS'
            ELSE 'FR_MR'
        END AS tool_group,
        af.customer_status,
        af.build_status,
        af.first_created_period,
        af.first_migrated_period,
        af.account_name,
        af.tenant_env_type,
        af.enterprise_size_group,
        af.segment,
        af.industry,
        af.super_industry,
        af.segment_size_l1,
        af.deployment_product_area,
        af.deployment_partner,
        af.deployment_type,
        af.deployment_phase,
        af.deployment_overall_status
    FROM activity_final af

    UNION ALL

    -- Section B: Active customer rows
    SELECT
        'B' AS src,
        CURRENT_DATE AS biweekly_period,
        acb.billing_id,
        acb.sf_account_id,
        CAST(NULL AS VARCHAR) AS user_type,
        CAST(NULL AS VARCHAR) AS tool_type,
        CAST(NULL AS VARCHAR) AS tool_group,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS VARCHAR) AS build_status,
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

    UNION ALL

    -- Section C: Active deployment rows
    SELECT
        'C' AS src,
        CURRENT_DATE AS biweekly_period,
        adb.billing_id,
        adb.sf_account_id,
        CAST(NULL AS VARCHAR) AS user_type,
        CAST(NULL AS VARCHAR) AS tool_type,
        CAST(NULL AS VARCHAR) AS tool_group,
        CAST(NULL AS VARCHAR) AS customer_status,
        CAST(NULL AS VARCHAR) AS build_status,
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
),

flagged_rows AS (
    SELECT
        src,
        biweekly_period, billing_id, sf_account_id,
        user_type, tool_type, tool_group, customer_status, build_status,
        first_created_period, first_migrated_period,
        account_name, tenant_env_type,
        enterprise_size_group, segment, industry, super_industry, segment_size_l1,
        deployment_product_area, deployment_partner, deployment_type,
        deployment_phase, deployment_overall_status,
        MAX(CASE WHEN src = 'A' THEN 1 ELSE 0 END) OVER (PARTITION BY sf_account_id) AS any_activity,
        MAX(CASE WHEN src = 'B' THEN 1 ELSE 0 END) OVER (PARTITION BY sf_account_id) AS any_active_cust,
        MAX(CASE WHEN src = 'C' THEN 1 ELSE 0 END) OVER (PARTITION BY sf_account_id) AS any_active_deploy
    FROM tagged_rows
)
