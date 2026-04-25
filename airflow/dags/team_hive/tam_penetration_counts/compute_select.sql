, pre_agg AS (
  SELECT
    sf_account_id,

    -- MARKET SEGMENT FLAGS
    MAX(CASE WHEN UPPER(COALESCE(deployment_phase, '')) LIKE '%LAUNCH%'
              OR UPPER(COALESCE(deployment_phase, '')) LIKE '%EXPRESS%'
         THEN 1 ELSE 0 END) AS has_launch_express,
    MAX(CASE WHEN enterprise_size_group = 'LE' THEN 1 ELSE 0 END) AS has_le_enterprise,
    MAX(CASE WHEN enterprise_size_group = 'ME' THEN 1 ELSE 0 END) AS has_me_enterprise,
    MAX(CASE WHEN
        UPPER(COALESCE(deployment_partner, '')) LIKE '%ALBIDA%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%APEX%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%TOPBLOC%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%BNB%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%BNET BUILDERS%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%BUSINESS NETWORK BUILDERS%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%HR PATH%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%KAINOS%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%KNOWBRIST%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%THREE LINK%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%MERCER%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%OKORIO%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%OKARIO%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%THREE PLUS%' OR
        UPPER(COALESCE(deployment_partner, '')) LIKE '%3PLUS%'
         THEN 1 ELSE 0 END) AS has_go_partner,
    MAX(CASE WHEN UPPER(sf_account_id) IN (SELECT UPPER(account_id) FROM cdt.workday_go_accounts)
         THEN 1 ELSE 0 END) AS has_go_customer,
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
  UNION ALL SELECT 'Launch/Express' AS market_segment
  UNION ALL SELECT 'LE' AS market_segment
  UNION ALL SELECT 'ME' AS market_segment
  UNION ALL SELECT 'GO Partners' AS market_segment
  UNION ALL SELECT 'GO Customers' AS market_segment
),
filtered AS (
  SELECT
    pa.*,
    ms.market_segment,
    CASE
      WHEN ms.market_segment = 'All' THEN 1
      WHEN ms.market_segment = 'Launch/Express' THEN has_launch_express
      WHEN ms.market_segment = 'LE' THEN has_le_enterprise
      WHEN ms.market_segment = 'ME' THEN has_me_enterprise
      WHEN ms.market_segment = 'GO Partners' THEN has_go_partner
      WHEN ms.market_segment = 'GO Customers' THEN has_go_customer
      ELSE 0
    END AS seg
  FROM pre_agg pa
  CROSS JOIN market_segments ms
)
SELECT
  market_segment,

  -- DENOMINATOR METRICS (4)
  SUM(seg * f_active_customer)      AS active_customer_count,
  SUM(seg * f_deploy_all)           AS active_deployment_count_all,
  SUM(seg * f_deploy_initial)       AS active_deployment_count_initial,
  SUM(seg * f_deploy_phase_x)       AS active_deployment_count_phase_x,

  -- OVERALL ACTIVITY - IMPLEMENTER (4)
  SUM(seg * f_init_tool)  AS activity_initial_tool_usage,
  SUM(seg * f_init_mig)   AS activity_initial_migrated,
  SUM(seg * f_px_tool)    AS activity_phase_x_tool_usage,
  SUM(seg * f_px_mig)     AS activity_phase_x_migrated,

  -- OVERALL ACTIVITY - CUSTOMER (2)
  SUM(seg * f_cust_all)     AS activity_customer_all,
  SUM(seg * f_cust_all_mig) AS activity_customer_all_migrated,

  -- CT_TC_AS GROUP - IMPLEMENTER (4)
  SUM(seg * f_ct_tc_init_tool) AS activity_initial_ct_tc_tool_usage,
  SUM(seg * f_ct_tc_init_mig)  AS activity_initial_ct_tc_migrated,
  SUM(seg * f_ct_tc_px_tool)   AS activity_phase_x_ct_tc_tool_usage,
  SUM(seg * f_ct_tc_px_mig)    AS activity_phase_x_ct_tc_migrated,

  -- CT_TC_AS GROUP - CUSTOMER (2)
  SUM(seg * f_cust_ct_tc)     AS activity_customer_ct_tc,
  SUM(seg * f_cust_ct_tc_mig) AS activity_customer_ct_tc_migrated,

  -- FR_MR GROUP - IMPLEMENTER (2)
  SUM(seg * f_fr_mr_init_mig) AS activity_initial_fr_mr_migrated,
  SUM(seg * f_fr_mr_px_mig)   AS activity_phase_x_fr_mr_migrated,

  -- CHANGE TRACKER - IMPLEMENTER (4)
  SUM(seg * f_ct_init_tool) AS activity_initial_ct_tool_usage,
  SUM(seg * f_ct_init_mig)  AS activity_initial_ct_migrated,
  SUM(seg * f_ct_px_tool)   AS activity_phase_x_ct_tool_usage,
  SUM(seg * f_ct_px_mig)    AS activity_phase_x_ct_migrated,

  -- CHANGE TRACKER - CUSTOMER (2)
  SUM(seg * f_cust_ct)     AS activity_customer_ct,
  SUM(seg * f_cust_ct_mig) AS activity_customer_ct_migrated,

  -- TENANT COMPARE - IMPLEMENTER (4)
  SUM(seg * f_tc_init_tool) AS activity_initial_tc_tool_usage,
  SUM(seg * f_tc_init_mig)  AS activity_initial_tc_migrated,
  SUM(seg * f_tc_px_tool)   AS activity_phase_x_tc_tool_usage,
  SUM(seg * f_tc_px_mig)    AS activity_phase_x_tc_migrated,

  -- TENANT COMPARE - CUSTOMER (2)
  SUM(seg * f_cust_tc)     AS activity_customer_tc,
  SUM(seg * f_cust_tc_mig) AS activity_customer_tc_migrated,

  -- ADHOC SCOPE - IMPLEMENTER (4)
  SUM(seg * f_as_init_tool) AS activity_initial_as_tool_usage,
  SUM(seg * f_as_init_mig)  AS activity_initial_as_migrated,
  SUM(seg * f_as_px_tool)   AS activity_phase_x_as_tool_usage,
  SUM(seg * f_as_px_mig)    AS activity_phase_x_as_migrated,

  -- ADHOC SCOPE - CUSTOMER (2)
  SUM(seg * f_cust_as)     AS activity_customer_as,
  SUM(seg * f_cust_as_mig) AS activity_customer_as_migrated,

  -- FOUNDATION RECIPE - IMPLEMENTER (2)
  SUM(seg * f_fr_init_mig) AS activity_initial_fr_migrated,
  SUM(seg * f_fr_px_mig)   AS activity_phase_x_fr_migrated,

  -- MIGRATION RECIPE - IMPLEMENTER (2)
  SUM(seg * f_mr_init_mig) AS activity_initial_mr_migrated,
  SUM(seg * f_mr_px_mig)   AS activity_phase_x_mr_migrated,
  -- METADATA
  '{{ comments }}' AS comments,
  DATE_TRUNC('month', DATE_ADD('month', -5, DATE '{{ current_date }}')) AS window_start,
  CAST(CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP) AS computed_at,
  DATE '{{ current_date }}' AS snapshot_date

FROM filtered
GROUP BY market_segment
