-- =============================================================================
-- DOCUMENTATION (exclude this section when copying to Tableau)
-- =============================================================================
-- Changes from previous version:
--   - Date filter changed from hardcoded dates to relative 6-month rolling window
--   - Changed from monthly (wd_event_month) to semi-monthly (biweekly_period)
--   - Semi-monthly logic: Days 1-15 → 1st of month, Days 16-31 → 16th of month
--   - Start date: First day of the month, 6 months ago
--   - End date: Excludes current incomplete period
-- =============================================================================


-- =============================================================================
-- MIGRATION SUCCESS RATE BY CLIENT DATASOURCE
-- =============================================================================
-- Purpose: Powers Transaction DataLoad Readiness Rate chart
-- Grain: One row per tool per biweekly_period with aggregated error/record counts
--
-- Tools Included:
--   - Migration Recipe (from DATALOAD_METRICS)
--   - OX - Configuration Package (from migrations_blended)
--   - OX - Materialized Scope (from migrations_blended)
--   - CloudLoader AL - Validation (from DATALOAD_METRICS)
--   - CloudLoader AL - Load (from DATALOAD_METRICS)
--   - Foundation Recipe (from DATALOAD_METRICS)
--
-- Success Rate Calculation in Tableau:
--   1 - (SUM([error_count]) / SUM([total_record_count]))
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
)

-- Migration Recipe
SELECT 
    'Migration Recipe' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(total_failed_records) AS error_count,
    SUM(total_records) AS total_record_count
FROM dw.SWH.DATALOAD_METRICS
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND client_id IN ('Ox Sweep - Migration') 
  AND IS_VALIDATE = false
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END

UNION ALL

-- OX - Configuration Package
SELECT 
    'OX - Configuration Package' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(count_push_error) AS error_count,
    SUM(count_total_instances) AS total_record_count
FROM dw.cdt.migrations_blended
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND metric_type IN ('push', 'push_batch', 'error') 
  AND source_object_type = 'Configuration Package'
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END

UNION ALL

-- OX - Materialized Scope
SELECT 
    'OX - Materialized Scope' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(count_push_error) AS error_count,
    SUM(count_total_instances) AS total_record_count
FROM dw.cdt.migrations_blended
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND metric_type IN ('push', 'push_batch', 'error') 
  AND source_object_type = 'Materialized Scope'
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END

UNION ALL

-- CloudLoader AL - Validation
SELECT 
    'CloudLoader AL - Validation' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(total_failed_records) AS error_count,
    SUM(total_records) AS total_record_count
FROM dw.SWH.DATALOAD_METRICS
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND client_id IN ('CloudLoader AL') 
  AND IS_VALIDATE = true
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END

UNION ALL

-- CloudLoader AL - Load
SELECT 
    'CloudLoader AL - Load' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(total_failed_records) AS error_count,
    SUM(total_records) AS total_record_count
FROM dw.SWH.DATALOAD_METRICS
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND client_id IN ('CloudLoader AL') 
  AND IS_VALIDATE = false
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END

UNION ALL

-- Foundation Recipe
SELECT 
    'Foundation Recipe' AS "Tools",
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END AS biweekly_period,
    SUM(total_failed_records) AS error_count,
    SUM(total_records) AS total_record_count
FROM dw.SWH.DATALOAD_METRICS
CROSS JOIN date_range dr
WHERE CAST(wd_event_date AS DATE) >= dr.start_date
  AND CAST(wd_event_date AS DATE) < dr.end_date
  AND client_id IN ('Ox Sweep - Foundation') 
  AND IS_VALIDATE = false
GROUP BY 
    CASE 
        WHEN DAY(CAST(wd_event_date AS DATE)) <= 15 
            THEN DATE_ADD('day', 14, DATE_TRUNC('month', CAST(wd_event_date AS DATE)))
        ELSE DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, CAST(wd_event_date AS DATE))))
    END


-- =============================================================================
-- TABLEAU CALCULATED FIELD
-- =============================================================================
-- 
-- Success Rate:
--   1 - (SUM([error_count]) / SUM([total_record_count]))
--
-- =============================================================================
