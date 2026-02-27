-- =============================================================================
-- Trino: Migration Tool Usage per Customer Account
-- =============================================================================
-- Purpose: Classifies which migration tools each customer account has used
--          based on push_migration events. Outputs one row per account × tool.
--
-- Classification Logic (uses scope_selection_type + input_type):
--   scope_selection_type = 'Input Selection Type'
--       → Tool from scopes_input_type_metrics.input_type (e.g., Change Tracker, Tenant Compare)
--   scope_selection_type = 'Manual Selection Type'
--       → 'Adhoc Scope'
--   scope_selection_type = 'Union Selection Type'
--       → BOTH tool(s) from input_type AND 'Adhoc Scope' (multiple rows per scope)
--   scope_selection_type IS NULL / scope not found
--       → 'Adhoc Scope'
--
-- Tool Type Mapping (from input_type column):
--   'Change Tracker Scope' → 'Change Tracker'
--   'Tenant Compare Scope' → 'Tenant Compare'
--   Future values          → Dynamically picked up from input_type
--
-- Grain: One row per account_id × tool_type (deduplicated)
-- Time Range: All-time (no date filter)
-- User Type: Customer only (Implementer logic to be added separately)
--
-- Tableau Join: FULL OUTER JOIN to redshift_active_customers on account_id
--               This query should be joined BEFORE other Trino lookup queries.
--
-- Tableau Formula (single data source, no blend needed):
--   Penetration % = COUNTD(IF NOT ISNULL([tool_type]) THEN [account_id] END)
--                   / COUNTD([account_id]) * 100
-- =============================================================================

WITH 
-- =============================================================================
-- PARAMETERS: Partition pruning bounds (rolling 12 months)
-- =============================================================================
Parameters AS (
    SELECT 
        DATE_TRUNC('month', DATE_ADD('month', -12, CURRENT_DATE)) AS start_date,
        DATE_TRUNC('month', CURRENT_DATE) AS end_date
),

-- =============================================================================
-- BASE: All push_migration events (Customer only, all-time)
-- =============================================================================
push_migrations AS (
    SELECT DISTINCT
        m.source_object_id,
        m.cc_billing_id
    FROM dw.swh.migration_event_log m
    CROSS JOIN Parameters p
    WHERE m.event_type = 'push_migration'
      AND m.user_type = 'Customer'
      AND m.source_object_type = 'Materialized Scope'
      AND m.cc_billing_id IS NOT NULL
      AND m.wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND m.wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- LOOKUP: Scope → Input type from scopes_input_type_metrics
-- Distinct input_type per scope (handles future multi-input scopes)
-- =============================================================================
scope_input_types AS (
    SELECT DISTINCT
        scope_external_id,
        input_type
    FROM dw.swh.scopes_input_type_metrics
    CROSS JOIN Parameters p
    WHERE input_type IS NOT NULL
      AND wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- LOOKUP: Scope selection type from scopes_metrics
-- =============================================================================
scope_selection_types AS (
    SELECT DISTINCT
        scope_external_id,
        scope_selection_type
    FROM dw.swh.scopes_metrics
    CROSS JOIN Parameters p
    WHERE wd_event_date >= format_datetime(p.start_date, 'yyyy-MM-dd')
      AND wd_event_date < format_datetime(p.end_date, 'yyyy-MM-dd')
),

-- =============================================================================
-- STREAM 1: Input-based tool rows
-- Covers: Input Selection Type AND input portion of Union Selection Type
-- Any scope found in scopes_input_type_metrics gets a row per input_type
-- =============================================================================
input_tool_migrations AS (
    SELECT DISTINCT
        pm.cc_billing_id,
        CASE sit.input_type
            WHEN 'Change Tracker Scope' THEN 'Change Tracker'
            WHEN 'Tenant Compare Scope' THEN 'Tenant Compare'
            ELSE sit.input_type
        END AS tool_type
    FROM push_migrations pm
    INNER JOIN scope_input_types sit 
        ON pm.source_object_id = sit.scope_external_id
),

-- =============================================================================
-- STREAM 2: Adhoc Scope rows
-- Covers:
--   - Manual Selection Type   → always Adhoc
--   - Union Selection Type    → Adhoc (in addition to input tool from Stream 1)
--   - NULL selection type     → Adhoc
--   - Scope not in scopes_metrics at all → Adhoc
-- =============================================================================
adhoc_migrations AS (
    SELECT DISTINCT
        pm.cc_billing_id,
        'Adhoc Scope' AS tool_type
    FROM push_migrations pm
    LEFT JOIN scope_selection_types sst 
        ON pm.source_object_id = sst.scope_external_id
    WHERE sst.scope_selection_type IN ('Manual Selection Type', 'Union Selection Type')
       OR sst.scope_selection_type IS NULL
),

-- =============================================================================
-- COMBINE: Both streams
-- A Union scope will appear in BOTH streams (input tool + Adhoc)
-- UNION ALL preserves this intentional duplication before final DISTINCT
-- =============================================================================
all_tool_migrations AS (
    SELECT cc_billing_id, tool_type FROM input_tool_migrations
    UNION ALL
    SELECT cc_billing_id, tool_type FROM adhoc_migrations
),

-- =============================================================================
-- MAP: Billing ID → Account ID via SFDC lookup
-- =============================================================================
billing_to_account AS (
    SELECT DISTINCT
        sad.billing_id,
        sad.sf_account_id
    FROM dw.lookup_db.sfdc_account_details sad
    WHERE sad.billing_id IS NOT NULL
      AND sad.sf_account_id IS NOT NULL
)

-- =============================================================================
-- FINAL OUTPUT: One row per account × tool_type
-- =============================================================================
SELECT DISTINCT
    ba.sf_account_id AS account_id,
    atm.tool_type
FROM all_tool_migrations atm
INNER JOIN billing_to_account ba 
    ON atm.cc_billing_id = ba.billing_id
