, pre_agg AS (
  SELECT
    sf_account_id,

    -- MARKET SEGMENT FLAG
    MAX(CASE WHEN UPPER(COALESCE(deployment_phase, '')) LIKE '%LAUNCH%'
              OR UPPER(COALESCE(deployment_phase, '')) LIKE '%EXPRESS%'
         THEN 1 ELSE 0 END) AS has_le_row,

    -- DENOMINATOR FLAGS (4)
    MAX(CASE WHEN any_active_cust > 0 THEN 1 ELSE 0 END) AS f_active_customer,
    MAX(CASE WHEN any_active_deploy > 0
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      THEN 1 ELSE 0 END) AS f_deploy_all,
    MAX(CASE WHEN any_active_deploy > 0 AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      THEN 1 ELSE 0 END) AS f_deploy_initial,
    MAX(CASE WHEN any_active_deploy > 0 AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      THEN 1 ELSE 0 END) AS f_deploy_phase_x,

    -- OVERALL ACTIVITY - IMPLEMENTER FLAGS (4)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_init_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_px_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment')
      AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_px_mig,

    -- OVERALL ACTIVITY - CUSTOMER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' THEN 1 ELSE 0 END) AS f_cust_all,
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND customer_status = 'Migrated' THEN 1 ELSE 0 END) AS f_cust_all_mig,

    -- CT_TC_AS GROUP - IMPLEMENTER FLAGS (4)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'CT_TC_AS' AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_tc_init_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'CT_TC_AS' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_tc_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'CT_TC_AS' AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_tc_px_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'CT_TC_AS' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_tc_px_mig,

    -- CT_TC_AS GROUP - CUSTOMER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_group = 'CT_TC_AS' THEN 1 ELSE 0 END) AS f_cust_ct_tc,
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_group = 'CT_TC_AS' AND customer_status = 'Migrated' THEN 1 ELSE 0 END) AS f_cust_ct_tc_mig,

    -- FR_MR GROUP - IMPLEMENTER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'FR_MR' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_fr_mr_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_group = 'FR_MR' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_fr_mr_px_mig,

    -- CHANGE TRACKER - IMPLEMENTER FLAGS (4)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Change Tracker' AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_init_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Change Tracker' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Change Tracker' AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_px_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Change Tracker' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_ct_px_mig,

    -- CHANGE TRACKER - CUSTOMER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Change Tracker' THEN 1 ELSE 0 END) AS f_cust_ct,
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Change Tracker' AND customer_status = 'Migrated' THEN 1 ELSE 0 END) AS f_cust_ct_mig,

    -- TENANT COMPARE - IMPLEMENTER FLAGS (4)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Tenant Compare' AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_tc_init_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Tenant Compare' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_tc_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Tenant Compare' AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_tc_px_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Tenant Compare' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_tc_px_mig,

    -- TENANT COMPARE - CUSTOMER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Tenant Compare' THEN 1 ELSE 0 END) AS f_cust_tc,
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Tenant Compare' AND customer_status = 'Migrated' THEN 1 ELSE 0 END) AS f_cust_tc_mig,

    -- ADHOC SCOPE - IMPLEMENTER FLAGS (4)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Adhoc Scope' AND deployment_type = 'Initial Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_as_init_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Adhoc Scope' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_as_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Adhoc Scope' AND deployment_type = 'Subsequent Deployment'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_as_px_tool,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Adhoc Scope' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_as_px_mig,

    -- ADHOC SCOPE - CUSTOMER FLAGS (2)
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Adhoc Scope' THEN 1 ELSE 0 END) AS f_cust_as,
    MAX(CASE WHEN src = 'A' AND user_type = 'Customer' AND tool_type = 'Adhoc Scope' AND customer_status = 'Migrated' THEN 1 ELSE 0 END) AS f_cust_as_mig,

    -- FOUNDATION RECIPE - IMPLEMENTER FLAGS (2, migrated only)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Foundation Recipe' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_fr_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Foundation Recipe' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_fr_px_mig,

    -- MIGRATION RECIPE - IMPLEMENTER FLAGS (2, migrated only)
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Migration Recipe' AND deployment_type = 'Initial Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_mr_init_mig,
    MAX(CASE WHEN src = 'A' AND user_type = 'Implementer' AND tool_type = 'Migration Recipe' AND deployment_type = 'Subsequent Deployment'
      AND customer_status = 'Migrated'
      AND COALESCE(deployment_phase,'') NOT IN ('Customer Enablement','Customer Led','No Deployment')
      AND COALESCE(deployment_product_area,'') NOT IN ('No Deployment') AND COALESCE(segment,'') != 'US Federal'
      THEN 1 ELSE 0 END) AS f_mr_px_mig

  FROM flagged_rows
  WHERE src = 'A' OR src = 'B' OR (src = 'C' AND any_active_cust = 0)
  GROUP BY sf_account_id
),
market_segments AS (
  SELECT 'All' AS market_segment
  UNION ALL
  SELECT 'Launch/Express' AS market_segment
)
SELECT
  ms.market_segment,

  -- DENOMINATOR METRICS (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_active_customer ELSE 0 END) AS active_customer_count,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_deploy_all ELSE 0 END)      AS active_deployment_count_all,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_deploy_initial ELSE 0 END)  AS active_deployment_count_initial,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_deploy_phase_x ELSE 0 END)  AS active_deployment_count_phase_x,

  -- OVERALL ACTIVITY - IMPLEMENTER (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_init_tool ELSE 0 END) AS activity_initial_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_init_mig ELSE 0 END)  AS activity_initial_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_px_tool ELSE 0 END)   AS activity_phase_x_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_px_mig ELSE 0 END)    AS activity_phase_x_migrated,

  -- OVERALL ACTIVITY - CUSTOMER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_all ELSE 0 END)     AS activity_customer_all,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_all_mig ELSE 0 END) AS activity_customer_all_migrated,

  -- CT_TC_AS GROUP - IMPLEMENTER (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_tc_init_tool ELSE 0 END) AS activity_initial_ct_tc_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_tc_init_mig ELSE 0 END)  AS activity_initial_ct_tc_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_tc_px_tool ELSE 0 END)   AS activity_phase_x_ct_tc_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_tc_px_mig ELSE 0 END)    AS activity_phase_x_ct_tc_migrated,

  -- CT_TC_AS GROUP - CUSTOMER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_ct_tc ELSE 0 END)     AS activity_customer_ct_tc,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_ct_tc_mig ELSE 0 END) AS activity_customer_ct_tc_migrated,

  -- FR_MR GROUP - IMPLEMENTER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_fr_mr_init_mig ELSE 0 END) AS activity_initial_fr_mr_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_fr_mr_px_mig ELSE 0 END)   AS activity_phase_x_fr_mr_migrated,

  -- CHANGE TRACKER - IMPLEMENTER (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_init_tool ELSE 0 END) AS activity_initial_ct_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_init_mig ELSE 0 END)  AS activity_initial_ct_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_px_tool ELSE 0 END)   AS activity_phase_x_ct_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_ct_px_mig ELSE 0 END)    AS activity_phase_x_ct_migrated,

  -- CHANGE TRACKER - CUSTOMER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_ct ELSE 0 END)     AS activity_customer_ct,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_ct_mig ELSE 0 END) AS activity_customer_ct_migrated,

  -- TENANT COMPARE - IMPLEMENTER (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_tc_init_tool ELSE 0 END) AS activity_initial_tc_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_tc_init_mig ELSE 0 END)  AS activity_initial_tc_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_tc_px_tool ELSE 0 END)   AS activity_phase_x_tc_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_tc_px_mig ELSE 0 END)    AS activity_phase_x_tc_migrated,

  -- TENANT COMPARE - CUSTOMER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_tc ELSE 0 END)     AS activity_customer_tc,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_tc_mig ELSE 0 END) AS activity_customer_tc_migrated,

  -- ADHOC SCOPE - IMPLEMENTER (4)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_as_init_tool ELSE 0 END) AS activity_initial_as_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_as_init_mig ELSE 0 END)  AS activity_initial_as_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_as_px_tool ELSE 0 END)   AS activity_phase_x_as_tool_usage,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_as_px_mig ELSE 0 END)    AS activity_phase_x_as_migrated,

  -- ADHOC SCOPE - CUSTOMER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_as ELSE 0 END)     AS activity_customer_as,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_cust_as_mig ELSE 0 END) AS activity_customer_as_migrated,

  -- FOUNDATION RECIPE - IMPLEMENTER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_fr_init_mig ELSE 0 END) AS activity_initial_fr_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_fr_px_mig ELSE 0 END)   AS activity_phase_x_fr_migrated,

  -- MIGRATION RECIPE - IMPLEMENTER (2)
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_mr_init_mig ELSE 0 END) AS activity_initial_mr_migrated,
  SUM(CASE WHEN ms.market_segment = 'All' OR has_le_row = 1 THEN f_mr_px_mig ELSE 0 END)   AS activity_phase_x_mr_migrated,

  -- METADATA
  '{{ comments }}' AS comments,
  DATE_TRUNC('month', DATE_ADD('month', -6, CAST(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles' AS DATE))) AS window_start,
  CAST(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP) AS computed_at,
  CAST(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles' AS DATE) AS snapshot_date

FROM pre_agg
CROSS JOIN market_segments ms
GROUP BY ms.market_segment
