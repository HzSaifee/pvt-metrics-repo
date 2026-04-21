-- =============================================================================
-- TAM Adoption Trend Datasource - Custom SQL (Long Format)
-- =============================================================================
-- Purpose: Produces one row per (snapshot_month × metric) for the Tableau
--          Adoption Trend line chart. Penetration percentages and TAM targets
--          are computed entirely in SQL — no standalone Tableau calc fields needed.
--
-- Source Table: cdt.tam_penetration_counts
--   Pre-aggregated by the tam_penetration_counts Airflow DAG.
--   Segment filtering is baked in (market_segment column).
--
-- Architecture:
--   1. base CTE — aggregates snapshots to monthly grain per market_segment
--   2. CROSS JOIN VALUES — defines the 7 metric rows (metric_order, section,
--      metric_name, tam_target) that appear in each month
--   3. CASE on metric_order — picks the right numerator/denominator pair and
--      embeds the segment filter (Launch/Express or All) per metric
--
-- Output Columns:
--   snapshot_month  — first day of the month
--   metric_order    — display sort (1–7)
--   section         — grouping label (Initial Deployments / Phase X / Customer)
--   metric_name     — human-readable metric label
--   tam_target      — static target percentage (integer)
--   penetration_pct — computed penetration % (already × 100)
--
-- =============================================================================

WITH base AS (
  SELECT
    DATE_TRUNC('month', snapshot_date)       AS snapshot_month,
    market_segment,
    AVG(active_customer_count)               AS avg_active_customers,
    AVG(active_deployment_count_initial)     AS avg_deploy_initial,
    AVG(active_deployment_count_phase_x)     AS avg_deploy_phase_x,
    MAX(activity_initial_fr_mr_migrated)     AS max_initial_fr_mr_migrated,
    MAX(activity_initial_ct_tool_usage)      AS max_initial_ct_tool_usage,
    MAX(activity_initial_tc_tool_usage)      AS max_initial_tc_tool_usage,
    MAX(activity_phase_x_fr_mr_migrated)    AS max_phase_x_fr_mr_migrated,
    MAX(activity_phase_x_ct_tool_usage)     AS max_phase_x_ct_tool_usage,
    MAX(activity_phase_x_tc_tool_usage)     AS max_phase_x_tc_tool_usage,
    MAX(activity_customer_ct_tc)            AS max_customer_ct_tc
  FROM dw.cdt.tam_penetration_counts
  WHERE
    DATE_TRUNC('month', snapshot_date) < DATE_TRUNC('month', CURRENT_DATE)
    AND snapshot_date >= DATE_ADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
  GROUP BY
    DATE_TRUNC('month', snapshot_date),
    market_segment
)

-- =============================================================================
-- FINAL OUTPUT: One row per (snapshot_month × metric)
-- =============================================================================

SELECT
  b.snapshot_month,
  m.metric_order,
  m.section,
  m.metric_name,
  m.tam_target,
  CASE m.metric_order
    WHEN 1 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_initial_fr_mr_migrated END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_initial END), 0)
    WHEN 2 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_initial_ct_tool_usage END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_initial END), 0)
    WHEN 3 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_initial_tc_tool_usage END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_initial END), 0)
    WHEN 4 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_phase_x_fr_mr_migrated END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_phase_x END), 0)
    WHEN 5 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_phase_x_ct_tool_usage END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_phase_x END), 0)
    WHEN 6 THEN SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.max_phase_x_tc_tool_usage END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'Launch/Express' THEN b.avg_deploy_phase_x END), 0)
    WHEN 7 THEN SUM(CASE WHEN b.market_segment = 'All' THEN b.max_customer_ct_tc END)
                * 100.0 / NULLIF(SUM(CASE WHEN b.market_segment = 'All' THEN b.avg_active_customers END), 0)
  END AS penetration_pct
FROM base b
CROSS JOIN (
  VALUES
    (1, 'Initial Deployments', 'Migration Tools (FR/MR)',         75),
    (2, 'Initial Deployments', 'Change Tracker',                  95),
    (3, 'Initial Deployments', 'Tenant Compare',                  95),
    (4, 'Phase X',             'Migration Tools (FR/MR)',         40),
    (5, 'Phase X',             'Change Tracker',                  95),
    (6, 'Phase X',             'Tenant Compare',                  95),
    (7, 'Customer',            'Change Tracker / Tenant Compare', 95)
) AS m(metric_order, section, metric_name, tam_target)
GROUP BY b.snapshot_month, m.metric_order, m.section, m.metric_name, m.tam_target
ORDER BY b.snapshot_month, m.metric_order