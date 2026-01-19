-- =============================================================================
-- CONSOLIDATED TABLEAU QUERIES FOR AUTOMATED CONFIGURATION DASHBOARD
-- =============================================================================
-- This file contains 3 data sources for Tableau:
--   1. billing_activity_monthly - Main fact table for most charts
--   2. denominators_reference   - Reference table for penetration calculations
--   3. tool_events_monthly      - Aggregated events for "Used vs NOT Used" chart
--
-- Key Filters Available in Tableau:
--   - user_type: Customer / Implementer
--   - enterprise_size_group: ME / LE
--   - deployment_bucket: Initial Deployment / Phase X Deployment
--   - tool_type: Change Tracker / Tenant Compare / Manual
--   - tenant_env_type: PROD / IMPL / SANDBOX (all included; filter in Tableau)
--   - usage_status: Migrated / Created Only
--   - event_month: Date filter
--   - segment: Account segment (NEW)
--   - industry: Account industry (NEW)
--   - segment_size_l1: Account segment size L1 (NEW)
--
-- Notes:
--   - All queries use 12-month rolling window (filter to 6 months in Tableau)
--   - All migration joins filter for event_type = 'push_migration' for consistency
--   - Aggregation is by billing_id (not tenant_prefix)
--   - Running totals should be calculated in Tableau using table calculations
--   - Manual scopes = scopes in scopes_metrics but NOT in scopes_input_type_metrics
--
-- Performance Notes:
--   - LOWER() on tenant joins prevents index usage but is necessary for data quality
--   - Partition pruning via wd_event_date filters is critical for performance
--   - Recommended indexes on source tables:
--       * change_tracker_event_log: (wd_event_date, user_type, tenant, change_tracker_wid)
--       * tenant_compare_event_log: (wd_event_date, user_type, tenant, tenant_compare_scope_wid)
--       * scopes_input_type_metrics: (wd_event_date, input_id, scope_external_id)
--       * scopes_metrics: (wd_event_date, tenant_name, scope_external_id, user_type)
--       * migration_event_log: (wd_event_date, event_type, source_object_id)
-- =============================================================================


-- =============================================================================
-- DATA SOURCE 1: billing_activity_monthly
-- =============================================================================
-- Purpose: Main fact table powering most dashboard charts
-- Grain: One row per billing_id × month × user_type × tool_type × usage_status
-- 
-- Supports Charts:
--   - Unique Accounts using Tooling (Initial vs Phase X) - Both Customer & Implementer
--   - Unique Customer/Implementer Change Tracker Migrations Usage ME/LE
--   - Enterprise Size Group pie charts
--   - Customer/Implementer Utilization: Change Trackers for Migration
--   - Month-over-Month cumulative charts
--
-- Tool Types:
--   - Change Tracker: Scopes created from Change Tracker input
--   - Tenant Compare: Scopes created from Tenant Compare input
--   - Manual: Scopes created without CT/TC input (exist in scopes_metrics but not scopes_input_type_metrics)
--
-- Tableau Calculations Needed:
--   - Cumulative counts: Use RUNNING_SUM() table calculation
--   - Penetration %: Join to denominators_reference data source
-- =============================================================================

WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE)) AS start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS end_date
),

-- =============================================================================
-- STEP 1: Extract Change Tracker Events with Migration Link Status
-- =============================================================================
-- NOTE on dual date filters (wd_event_date AND ct.time):
--   - wd_event_date: VARCHAR partition column - REQUIRED for Trino partition pruning
--   - ct.time: TIMESTAMP column - used for accurate month bucketing
--   Both filters are necessary: wd_event_date for performance, ct.time for correctness.
--   They should align but wd_event_date is date-only while ct.time has full timestamp.
-- =============================================================================
change_tracker_events AS (
    SELECT 
        date_trunc('month', ct.time) AS event_month,
        ct.user_type,
        ct.tenant,
        ct.change_tracker_wid,
        -- Determine if this CT was used in a push_migration
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    -- Link Change Tracker to Scope (via input_id)
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        ct.change_tracker_wid = s.input_id
        AND s.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND s.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    -- Link Scope to Migration (only push_migration events)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'  -- Consistent filter across all queries
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning for change_tracker_event_log (REQUIRED for Trino)
        ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter for accurate month bucketing
        AND ct.time >= p.start_date
        AND ct.time < p.end_date
        -- User type filter
        AND ct.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 2: Extract Tenant Compare Events with Migration Link Status
-- =============================================================================
tenant_compare_events AS (
    SELECT 
        date_trunc('month', tc.time) AS event_month,
        tc.user_type,
        tc.tenant,
        tc.tenant_compare_scope_wid,
        -- Determine if this TC was used in a push_migration
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    -- Link Tenant Compare to Scope (via input_id)
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        tc.tenant_compare_scope_wid = s.input_id
        AND s.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND s.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    -- Link Scope to Migration (only push_migration events)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning for tenant_compare_event_log
        tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter
        AND tc.time >= p.start_date
        AND tc.time < p.end_date
        -- User type filter
        AND tc.user_type IN ('Customer', 'Implementer')
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 3: Identify Scopes with CT/TC Input (for Manual Scope exclusion)
-- =============================================================================
-- A scope is "Manual" if it exists in scopes_metrics but NOT in scopes_input_type_metrics
scopes_with_input AS (
    SELECT DISTINCT scope_external_id
    FROM swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- STEP 4: Extract Manual Scope Events with Migration Link Status
-- =============================================================================
-- Manual scopes: exist in scopes_metrics but NOT in scopes_input_type_metrics
-- "Created" = appears in scopes_metrics
-- "Migrated" = also appears in migration_event_log with push_migration
manual_scope_events AS (
    SELECT 
        date_trunc('month', sm.time) AS event_month,
        sm.user_type,
        sm.tenant_name AS tenant,
        sm.scope_external_id,
        -- Determine if this manual scope was used in a push_migration
        MAX(CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END) AS has_migration_link
    FROM swh.scopes_metrics sm
    CROSS JOIN Parameters p
    -- Exclude scopes that have CT/TC input (those are not manual)
    LEFT JOIN scopes_with_input swi ON sm.scope_external_id = swi.scope_external_id
    -- Link to Migration (only push_migration events)
    LEFT JOIN swh.migration_event_log m ON (
        sm.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning for scopes_metrics
        sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter
        AND sm.time >= p.start_date
        AND sm.time < p.end_date
        -- User type filter
        AND sm.user_type IN ('Customer', 'Implementer')
        -- MANUAL SCOPE: No input type record exists
        AND swi.scope_external_id IS NULL
    GROUP BY 1, 2, 3, 4
),

-- =============================================================================
-- STEP 5: Combine Change Tracker, Tenant Compare, and Manual Events
-- =============================================================================
combined_tool_events AS (
    -- Change Tracker events
    SELECT 
        event_month, 
        user_type, 
        tenant, 
        'Change Tracker' AS tool_type, 
        has_migration_link
    FROM change_tracker_events
    
    UNION ALL
    
    -- Tenant Compare events
    SELECT 
        event_month, 
        user_type, 
        tenant, 
        'Tenant Compare' AS tool_type, 
        has_migration_link
    FROM tenant_compare_events
    
    UNION ALL
    
    -- Manual Scope events
    SELECT 
        event_month, 
        user_type, 
        tenant, 
        'Manual' AS tool_type, 
        has_migration_link
    FROM manual_scope_events
),

-- =============================================================================
-- STEP 6: Pre-calculate Phase X Deployment Flag per Account
-- NOTE: Accounts with NO deployments or ONLY "Initial Deployment" type
--       will have has_phase_x = NULL, which maps to 'Initial Deployment' bucket.
--       This is intentional business logic - absence of Phase X = Initial.
-- =============================================================================
phase_x_accounts AS (
    SELECT 
        customer_sf_account_id,
        1 AS has_phase_x
    FROM lookup_db.sfdc_deployments
    WHERE type != 'Initial Deployment'
    GROUP BY 1
),

-- =============================================================================
-- STEP 7: Join to SFDC Tables for Dimensions
-- =============================================================================
-- NOTE on LOWER() in tenant join:
--   Case-insensitive matching is required because tenant names in event logs
--   may have inconsistent casing compared to SFDC reference data.
--   This prevents index usage but is necessary for data quality (~98% match rate).
--   Consider ETL normalization if this becomes a performance bottleneck.
--
-- NOTE on tenant_env_type:
--   All tenant types (PROD/IMPL/SANDBOX) are included intentionally.
--   Filter in Tableau if you need to exclude SANDBOX for specific charts.
--
-- NEW COLUMNS: segment, industry, segment_size_l1 from sfdc_account_details
-- =============================================================================
enriched_events AS (
    SELECT 
        cte.event_month,
        cte.user_type,
        cte.tool_type,
        sfdc.billing_id,
        sfdc.tenant_type AS tenant_env_type,  -- PROD/IMPL/SANDBOX - filter in Tableau
        COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
        CASE 
            WHEN px.has_phase_x = 1 THEN 'Phase X Deployment'
            ELSE 'Initial Deployment'  -- NULL = no Phase X deployments = Initial
        END AS deployment_bucket,
        CASE 
            WHEN cte.has_migration_link = 1 THEN 'Migrated' 
            ELSE 'Created Only' 
        END AS usage_status,
        -- NEW: Additional account info for filtering
        COALESCE(sad.segment, 'Unknown') AS segment,
        COALESCE(sad.industry, 'Unknown') AS industry,
        COALESCE(sad.segment_size_l1, 'Unknown') AS segment_size_l1
    FROM combined_tool_events cte
    -- Join to SFDC Customer Tenants for billing_id (case-insensitive for data quality)
    INNER JOIN lookup_db.sfdc_customer_tenants sfdc 
        ON LOWER(cte.tenant) = LOWER(sfdc.tenant_name)
    -- Join to Account Details for enterprise_size_group and new columns
    LEFT JOIN lookup_db.sfdc_account_details sad 
        ON sfdc.sf_id = sad.sf_account_id
    -- Join to Phase X lookup
    LEFT JOIN phase_x_accounts px 
        ON sfdc.sf_id = px.customer_sf_account_id
    WHERE sfdc.billing_id IS NOT NULL
),

-- =============================================================================
-- STEP 8: Calculate First Activity Month per Billing ID & User Type
-- This enables "new customer this month" calculations in Tableau
-- =============================================================================
first_activity_lookup AS (
    SELECT 
        billing_id,
        user_type,
        MIN(event_month) AS first_activity_month
    FROM enriched_events
    GROUP BY 1, 2
),

-- =============================================================================
-- STEP 9: Aggregate to Final Grain
-- Grain: billing_id × month × user_type × tool_type × usage_status
-- =============================================================================
aggregated_activity AS (
    SELECT 
        e.event_month,
        e.user_type,
        e.tool_type,
        e.billing_id,
        e.tenant_env_type,
        e.enterprise_size_group,
        e.deployment_bucket,
        e.usage_status,
        e.segment,
        e.industry,
        e.segment_size_l1,
        fa.first_activity_month,
        CASE 
            WHEN e.event_month = fa.first_activity_month THEN 1 
            ELSE 0 
        END AS is_new_this_month,
        COUNT(*) AS event_count
    FROM enriched_events e
    LEFT JOIN first_activity_lookup fa 
        ON e.billing_id = fa.billing_id 
        AND e.user_type = fa.user_type
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)

-- =============================================================================
-- FINAL OUTPUT: billing_activity_monthly
-- =============================================================================
SELECT 
    event_month,
    user_type,
    tool_type,
    billing_id,
    tenant_env_type,
    enterprise_size_group,
    deployment_bucket,
    usage_status,
    segment,
    industry,
    segment_size_l1,
    first_activity_month,
    is_new_this_month,
    event_count
FROM aggregated_activity
ORDER BY event_month DESC, user_type, billing_id;


-- =============================================================================
-- DATA SOURCE 2: denominators_reference
-- =============================================================================
-- Purpose: Reference table for penetration percentage calculations
-- Usage: Join to billing_activity_monthly in Tableau for % calculations
--
-- Metrics:
--   - total_active_customers: Denominator for Customer penetration (~5,995)
--   - customers_with_active_deployments: Denominator for Implementer penetration (~2,485)
--
-- NOTE on deployment_start_date > '2023-01-01':
--   This is a FIXED business rule cutoff (not a rolling window) as specified by
--   stakeholders. It excludes legacy deployments from before 2023 to focus on
--   recent deployment activity. Update this date if business requirements change.
-- =============================================================================

SELECT 
    'total_active_customers' AS metric_name,
    'Customer' AS applies_to_user_type,
    COUNT(DISTINCT sf_id) AS metric_value,
    'Active Production tenants as of current month' AS description,
    CURRENT_DATE AS refresh_date  -- Added for data freshness tracking
FROM lookup_db.sfdc_customer_tenants
WHERE tenant_type = 'Production'
  AND status = 'Active'
  AND tenant_start_date < DATE_TRUNC('month', CURRENT_DATE)
  AND (
      tenant_expire_date IS NULL 
      OR tenant_expire_date >= DATE_TRUNC('month', CURRENT_DATE)
  )

UNION ALL

SELECT 
    'customers_with_active_deployments' AS metric_name,
    'Implementer' AS applies_to_user_type,
    COUNT(DISTINCT a.sf_account_id) AS metric_value,
    'Customers with active deployments (excl. Adhoc, etc.) started after 2023-01-01' AS description,
    CURRENT_DATE AS refresh_date  -- Added for data freshness tracking
FROM lookup_db.sfdc_account_details a
INNER JOIN lookup_db.sfdc_deployments d 
    ON a.sf_account_id = d.customer_sf_account_id
WHERE d.overall_status = 'Active'
  AND d.phase NOT IN (
      'Adhoc', 
      'Customer Enablement', 
      'Phase X - Sourcing', 
      'Customer Led', 
      'Peakon First', 
      'Sourcing First', 
      'Phase X - Peakon', 
      'Phase X - VNDLY', 
      'Phase X - Planning'
  )
  -- FIXED DATE: Business rule cutoff, not a rolling window
  AND d.deployment_start_date > DATE '2023-01-01';


-- =============================================================================
-- DATA SOURCE 3: tool_events_monthly
-- =============================================================================
-- Purpose: Aggregated event counts for "Migration Tools Used vs NOT Used" chart
-- Grain: One row per month × tool_type
-- 
-- Tool Types: Change Tracker, Tenant Compare, Manual
-- 
-- Note: This query specifically excludes sales/demo/gms tenants as per original
--       query logic for the Implementer "Used vs NOT Used" chart
-- =============================================================================

WITH Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE)) AS start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS end_date
),

-- Identify scopes with CT/TC input (for Manual exclusion)
scopes_with_input_ds3 AS (
    SELECT DISTINCT scope_external_id
    FROM swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- Change Tracker Events (Implementer only, excluding sales/demo/gms)
-- NOTE: used_for_migration checks for BOTH scope linkage AND push_migration event
--       to ensure consistency with Data Source 1 logic
ct_events AS (
    SELECT 
        date_trunc('month', ct.time) AS event_month,
        'Change Tracker' AS tool_type,
        ct.change_tracker_wid AS tool_event_id,
        -- FIXED: Now checks for actual push_migration event, not just scope linkage
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS used_for_migration
    FROM swh.change_tracker_event_log ct
    CROSS JOIN Parameters p
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        ct.change_tracker_wid = s.input_id
        AND s.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND s.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    -- Added: Link to migration_event_log for push_migration check (consistent with DS1)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning filter (required for Trino performance)
        ct.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND ct.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter for actual event timestamp
        AND ct.time >= p.start_date
        AND ct.time < p.end_date
        AND ct.user_type = 'Implementer'
        -- Exclude sales/demo/gms tenants (per original business logic)
        AND LOWER(ct.tenant) NOT LIKE '%sales%'
        AND LOWER(ct.tenant) NOT LIKE '%demo%'
        AND LOWER(ct.tenant) NOT LIKE '%gms%'
),

-- Tenant Compare Events (Implementer only, excluding sales/demo/gms)
-- NOTE: Same logic as CT - checks for actual push_migration event
tc_events AS (
    SELECT 
        date_trunc('month', tc.time) AS event_month,
        'Tenant Compare' AS tool_type,
        tc.tenant_compare_scope_wid AS tool_event_id,
        -- FIXED: Now checks for actual push_migration event, not just scope linkage
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS used_for_migration
    FROM swh.tenant_compare_event_log tc
    CROSS JOIN Parameters p
    LEFT JOIN swh.scopes_input_type_metrics s ON (
        tc.tenant_compare_scope_wid = s.input_id
        AND s.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND s.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    -- Added: Link to migration_event_log for push_migration check (consistent with DS1)
    LEFT JOIN swh.migration_event_log m ON (
        s.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning filter (required for Trino performance)
        tc.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND tc.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter for actual event timestamp
        AND tc.time >= p.start_date
        AND tc.time < p.end_date
        AND tc.user_type = 'Implementer'
        -- Exclude sales/demo/gms tenants (per original business logic)
        AND LOWER(tc.tenant) NOT LIKE '%sales%'
        AND LOWER(tc.tenant) NOT LIKE '%demo%'
        AND LOWER(tc.tenant) NOT LIKE '%gms%'
),

-- Manual Scope Events (Implementer only, excluding sales/demo/gms)
-- Manual = exists in scopes_metrics but NOT in scopes_input_type_metrics
manual_events AS (
    SELECT 
        date_trunc('month', sm.time) AS event_month,
        'Manual' AS tool_type,
        sm.scope_external_id AS tool_event_id,
        -- For manual scopes: used_for_migration = appeared in push_migration
        CASE WHEN m.event_id IS NOT NULL THEN 1 ELSE 0 END AS used_for_migration
    FROM swh.scopes_metrics sm
    CROSS JOIN Parameters p
    -- Exclude scopes with CT/TC input
    LEFT JOIN scopes_with_input_ds3 swi ON sm.scope_external_id = swi.scope_external_id
    -- Link to migration (push_migration only)
    LEFT JOIN swh.migration_event_log m ON (
        sm.scope_external_id = m.source_object_id
        AND m.event_type = 'push_migration'
        AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
    )
    WHERE 
        -- Partition pruning
        sm.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
        AND sm.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
        -- Time filter
        AND sm.time >= p.start_date
        AND sm.time < p.end_date
        AND sm.user_type = 'Implementer'
        -- Manual scope: no input type
        AND swi.scope_external_id IS NULL
        -- Exclude sales/demo/gms tenants
        AND LOWER(sm.tenant_name) NOT LIKE '%sales%'
        AND LOWER(sm.tenant_name) NOT LIKE '%demo%'
        AND LOWER(sm.tenant_name) NOT LIKE '%gms%'
),

-- Combine all three tool types
combined_events AS (
    SELECT * FROM ct_events
    UNION ALL
    SELECT * FROM tc_events
    UNION ALL
    SELECT * FROM manual_events
)

-- Final Aggregation
SELECT 
    event_month,
    tool_type,
    COUNT(*) AS total_events,
    SUM(used_for_migration) AS used_for_migration,
    SUM(CASE WHEN used_for_migration = 0 THEN 1 ELSE 0 END) AS not_used_for_migration
FROM combined_events
GROUP BY 1, 2
ORDER BY event_month DESC, tool_type;


-- =============================================================================
-- APPENDIX: Tableau Chart Mapping
-- =============================================================================
-- 
-- OUTPUT SCHEMA (Data Source 1):
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │ Column                 │ Values                    │ Notes                  │
-- ├─────────────────────────────────────────────────────────────────────────────┤
-- │ event_month            │ DATE                      │ Month of activity      │
-- │ user_type              │ Customer / Implementer    │ Section filter         │
-- │ tool_type              │ Change Tracker / Tenant Compare / Manual │ NEW!    │
-- │ billing_id             │ VARCHAR                   │ Unique customer ID     │
-- │ tenant_env_type        │ PROD / IMPL / SANDBOX     │ Environment filter     │
-- │ enterprise_size_group  │ ME / LE / Unknown         │ Size filter            │
-- │ deployment_bucket      │ Initial / Phase X         │ Deployment filter      │
-- │ usage_status           │ Migrated / Created Only   │ Utilization filter     │
-- │ segment                │ VARCHAR                   │ NEW! Account segment   │
-- │ industry               │ VARCHAR                   │ NEW! Account industry  │
-- │ segment_size_l1        │ VARCHAR                   │ NEW! Segment size L1   │
-- │ first_activity_month   │ DATE                      │ First activity date    │
-- │ is_new_this_month      │ 0 / 1                     │ New customer flag      │
-- │ event_count            │ INTEGER                   │ Event count            │
-- └─────────────────────────────────────────────────────────────────────────────┘
--
-- CUSTOMER SECTION:
-- ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
-- │ Chart Name                                    │ Data Source │ Filters/Dims                              │
-- ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ Unique Accounts (Initial vs Phase X)          │ DS1         │ user_type=Customer, deployment_bucket     │
-- │ Penetration Percentage                        │ DS1 + DS2   │ user_type=Customer                        │
-- │ Total Number of Active Customers              │ DS2         │ metric_name='total_active_customers'      │
-- │ Unique Customer CT Migrations ME/LE           │ DS1         │ user_type=Customer, enterprise_size_group │
-- │ Enterprise Size Group (Pie)                   │ DS1         │ user_type=Customer, enterprise_size_group │
-- │ Customer Utilization (Migrated vs Created)    │ DS1         │ user_type=Customer, usage_status          │
-- │ Month-over-Month Customers                    │ DS1         │ user_type=Customer, is_new_this_month=1   │
-- └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
--
-- IMPLEMENTER SECTION:
-- ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
-- │ Chart Name                                    │ Data Source │ Filters/Dims                                    │
-- ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ Unique Accounts (Initial vs Phase X)          │ DS1         │ user_type=Implementer, deployment_bucket        │
-- │ Percentage Penetration                        │ DS1 + DS2   │ user_type=Implementer                           │
-- │ Total Number of Active Deployments            │ DS2         │ metric_name='customers_with_active_deployments' │
-- │ Customers with Active Deployments             │ DS2         │ metric_name='customers_with_active_deployments' │
-- │ Unique Accounts ME/LE                         │ DS1         │ user_type=Implementer, enterprise_size_group    │
-- │ Enterprise Size Group (Pie)                   │ DS1         │ user_type=Implementer, enterprise_size_group    │
-- │ Implementer Utilization (Migrated vs Created) │ DS1         │ user_type=Implementer, usage_status             │
-- │ Migration Tools Used vs NOT Used              │ DS3         │ tool_type (CT/TC/Manual)                        │
-- └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
--
-- =============================================================================
-- NEW FILTER OPTIONS:
-- =============================================================================
--
-- tool_type filter allows comparing:
--   - "Change Tracker" only (original strict view)
--   - "Tenant Compare" only
--   - "Manual" only (scopes created without CT/TC input)
--   - All three combined (explains higher numbers vs CT/TC-only)
--
-- Additional account filters (NEW):
--   - segment: Filter by account segment
--   - industry: Filter by account industry
--   - segment_size_l1: Filter by segment size level 1
--
-- =============================================================================
-- TABLEAU CALCULATED FIELDS NEEDED:
-- =============================================================================
--
-- 1. Cumulative Unique Billing IDs (for bar charts):
--    RUNNING_SUM(COUNTD([billing_id]))
--    Compute using: Table (Across then Down) or specific dimension
--
-- 2. Penetration Percentage (Customer):
--    COUNTD([billing_id]) / [total_active_customers] * 100
--    Where [total_active_customers] comes from DS2 joined on applies_to_user_type
--
-- 3. Penetration Percentage (Implementer):
--    COUNTD([billing_id]) / [customers_with_active_deployments] * 100
--
-- 4. New Customers This Month:
--    Filter: [is_new_this_month] = 1
--    Then: COUNTD([billing_id])
--
-- 5. Tool Type Filter (NEW):
--    Use as filter to show CT-only, TC-only, Manual-only, or All
--    Default to "All" to see complete picture
--
-- =============================================================================
