-- =============================================================================
-- DOCUMENTATION (exclude this section when copying to Tableau)
-- =============================================================================
-- Changes from previous version:
--   - Table changed from dataload_metrics_deployment_data to dataload_metrics_deployment_data_airflow
--   - Date filter changed from hardcoded dates to relative 6-month rolling window
--   - Added biweekly_period column (semi-monthly grain) derived from event_date
--   - Semi-monthly logic: Days 1-15 → 1st of month, Days 16-31 → 16th of month
--   - Start date: First day of the month, 6 months ago
--   - End date: Excludes current incomplete period (same as FR-MR queries)
-- =============================================================================


-- =============================================================================
-- DC LOAD TOOL DATASOURCE
-- =============================================================================
-- Tableau Datasource Name: AC_DC_Load_Tool
-- Logical Table Name: DC_Load_Tool
-- Custom SQL Name: DC Load Tool Query
-- Workday GO Join: LEFT JOIN on sf_account_id = accountid (using redshift_workday_go.sql)
--
-- Purpose: Powers DC Load Tool dashboard tab with implementation metrics
-- Grain: One row per distinct dataload event with deployment context
--
-- NOTE: SELECT DISTINCT applied to deduplicate rows (~95% reduction from 142M to ~7M rows)
--
-- Filter Columns Available:
--   - segment, super_industry (from sfdc_account_details)
--   - deployment_type (default: Initial Deployment)
--   - implementation_partner (renamed from initial_deployment_partner)
--   - service_methodology (renamed from deployment_offering, default: all Launch offerings)
--   - data_type_group (Configuration/Transactional)
--   - tool_name, is_validate
--   - implementation_type (new filter)
--   - account_name (new filter)
--
-- Service Methodology Values:
--   - Launch Express (default selected)
--   - Launch Now (default selected)
--   - Launch Flex (default selected)
--   - Launch (default selected)
--   - Your Way
--   - Other
--
-- =============================================================================

WITH date_range AS (
    SELECT 
        -- Start date: First day of the month, 6 months ago
        DATE_TRUNC('month', DATE_ADD('month', -6, CURRENT_DATE)) AS start_date,
        -- End date: Current semi-monthly period (exclusive) - excludes incomplete period
        CASE 
            WHEN DAY(CURRENT_DATE) <= 15 
                THEN DATE_TRUNC('month', CURRENT_DATE)
            ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
        END AS end_date
),

type_mapping AS (
    SELECT 
        entity_name,
        category
    FROM dw.cdt.implementation_type_mapping
),

raw_data AS (
    SELECT 
        CASE 
            WHEN client_id IS NULL OR client_id = '' THEN 'Unknown' 
            ELSE client_id 
        END AS tool_name,
        implementation_type,
        implementation_component,
        CAST(wd_event_date AS DATE) AS event_date,
        -- Semi-monthly period (biweekly_period)
        CASE 
            WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
                THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
            ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
        END AS biweekly_period,
        initial_deployment_partner,
        deployment_name,
        deployment_type,
        customer_tenant,
        customer_tenant_prefix,
        account_name,
        is_validate,
        TRY_CAST(total_records AS DOUBLE) AS total_records,
        TRY_CAST(total_failed_records AS DOUBLE) AS total_failed_records
    FROM 
        dw.cdt.dataload_metrics_deployment_data_airflow
    CROSS JOIN date_range dr
    WHERE 
        CAST(wd_event_date AS DATE) >= dr.start_date
        AND CAST(wd_event_date AS DATE) < dr.end_date
)

SELECT DISTINCT
    m.event_date,
    m.biweekly_period,
    m.tool_name,
    m.customer_tenant,
    m.customer_tenant_prefix,
    m.account_name,
    sfdc.sf_account_id,
    sfdc.billing_id,
    CASE 
        WHEN map.category = 'Configuration' THEN 'Configuration'
        ELSE 'Transactional'
    END AS data_type_group,
    m.implementation_type,
    m.implementation_component,
    COALESCE(m.initial_deployment_partner, 'Unknown') AS initial_deployment_partner,
    COALESCE(m.deployment_type, 'Unknown') AS deployment_type,
    CASE 
        WHEN LOWER(m.deployment_name) LIKE '%launch express%' THEN 'Launch Express'
        WHEN LOWER(m.deployment_name) LIKE '%launch now%'     THEN 'Launch Now'
        WHEN LOWER(m.deployment_name) LIKE '%launch flex%'    THEN 'Launch Flex'
        WHEN LOWER(m.deployment_name) LIKE '%launch%'         THEN 'Launch'
        WHEN LOWER(m.deployment_name) LIKE '%your way%'       THEN 'Your Way'
        ELSE m.deployment_name
    END AS deployment_offering,
    m.deployment_name,
    COALESCE(sfdc.segment, 'Unknown') AS segment,
    COALESCE(sfdc.super_industry, 'Unknown') AS super_industry,
    
    -- Additional dimensions from sfdc_account_details (for potential future use)
    COALESCE(sfdc.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sfdc.industry, 'Unknown') AS industry,
    
    m.is_validate,
    m.total_records,
    m.total_failed_records

FROM 
    raw_data m
LEFT JOIN 
    type_mapping map 
    ON m.implementation_type = map.entity_name
LEFT JOIN 
    dw.lookup_db.sfdc_account_details sfdc
    ON m.account_name = sfdc.account_name


-- =============================================================================
-- TABLEAU FILTER DEFAULTS
-- =============================================================================
-- 
-- Deployment Type:
--   Default: "Initial Deployment" (single select)
--
-- Service Methodology:
--   Default: ["Launch Express", "Launch Now", "Launch Flex", "Launch"] (multi-select)
--   Note: "Your Way" and "Other" available but not selected by default
--
-- Implementation Partner:
--   Default: All (no filter applied)
--
-- Segment:
--   Default: All (no filter applied)
--
-- Super Industry:
--   Default: All (no filter applied)
--
-- Implementation Type:
--   Default: All (no filter applied)
--
-- Account Name:
--   Default: All (no filter applied)
--
-- =============================================================================


-- =============================================================================
-- WORKDAY GO JOIN (in Tableau)
-- =============================================================================
-- 
-- Join Type: LEFT JOIN
-- Join Condition: [sf_account_id] = [accountid]
-- Source: redshift_workday_go.sql query result
--
-- Calculated Field - Is Workday Go Customer?:
--   NOT ISNULL([accountid])
--
-- =============================================================================
