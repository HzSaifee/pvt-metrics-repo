-- =============================================================================
-- TAM Penetration Counts Datasource - OVERALL VIEW
-- =============================================================================
-- Purpose: Current-period penetration counts per market segment for the
--          TAM Penetration Counts Tableau dashboard (bar/KPI view).
--
-- Source Table: cdt.tam_penetration_counts
--   Pre-aggregated by the tam_penetration_counts Airflow DAG.
--   Segment membership is pre-computed in the SQL (market_segment column).
--
-- Architecture:
--   Single-pass aggregate over the latest biweekly period:
--     - AVG for denominators (active customers / deployments)
--     - MAX for numerators (activity counts)
--   Grouped by market_segment so Tableau can filter via [market_segment] = [Market].
--
-- Period Logic:
--   Day-of-month <= 15 → previous month window
--   Day-of-month >  15 → current month through the 15th
--
-- Output Columns:
--   market_segment              — segment label (All, LE, ME, GO Partners, Launch/Express)
--   period_start / period_end   — snapshot window boundaries (debugging / "Data as of")
--   avg_active_customers, avg_deploy_* — denominators (AVG of period)
--   max_*                       — numerators (MAX of period)
--
-- =============================================================================

SELECT
  market_segment,

  -- Period boundaries (for debugging / "Data as of" label)
  CASE
    WHEN DAY(CURRENT_DATE) <= 15
    THEN DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))
    ELSE DATE_TRUNC('month', CURRENT_DATE)
  END AS period_start,
  CASE
    WHEN DAY(CURRENT_DATE) <= 15
    THEN DATE_TRUNC('month', CURRENT_DATE)
    ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
  END AS period_end,

  -- Denominators (AVG of period)
  AVG(active_customer_count)             AS avg_active_customers,
  AVG(active_deployment_count_all)       AS avg_deploy_all,
  AVG(active_deployment_count_initial)   AS avg_deploy_initial,
  AVG(active_deployment_count_phase_x)   AS avg_deploy_phase_x,

  -- All Automated numerators (MAX of period)
  MAX(activity_initial_tool_usage)       AS max_initial_tool_usage,
  MAX(activity_initial_migrated)         AS max_initial_migrated,
  MAX(activity_phase_x_tool_usage)       AS max_phase_x_tool_usage,
  MAX(activity_phase_x_migrated)         AS max_phase_x_migrated,

  -- CT_TC_AS group numerators (MAX)
  MAX(activity_initial_ct_tc_tool_usage) AS max_initial_ct_tc_tool_usage,
  MAX(activity_initial_ct_tc_migrated)   AS max_initial_ct_tc_migrated,
  MAX(activity_phase_x_ct_tc_tool_usage) AS max_phase_x_ct_tc_tool_usage,
  MAX(activity_phase_x_ct_tc_migrated)   AS max_phase_x_ct_tc_migrated,

  -- Customer numerators (MAX)
  MAX(activity_customer_ct_tc)           AS max_customer_ct_tc,
  MAX(activity_customer_ct_tc_migrated)  AS max_customer_ct_tc_migrated,

  -- FR_MR group numerators (MAX)
  MAX(activity_initial_fr_mr_migrated)   AS max_initial_fr_mr_migrated,
  MAX(activity_phase_x_fr_mr_migrated)   AS max_phase_x_fr_mr_migrated

FROM cdt.tam_penetration_counts
WHERE
  snapshot_date >= CASE
    WHEN DAY(CURRENT_DATE) <= 15
    THEN DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))
    ELSE DATE_TRUNC('month', CURRENT_DATE)
  END
  AND snapshot_date < CASE
    WHEN DAY(CURRENT_DATE) <= 15
    THEN DATE_TRUNC('month', CURRENT_DATE)
    ELSE DATE_ADD('day', 15, DATE_TRUNC('month', CURRENT_DATE))
  END
GROUP BY
  market_segment
ORDER BY
  market_segment