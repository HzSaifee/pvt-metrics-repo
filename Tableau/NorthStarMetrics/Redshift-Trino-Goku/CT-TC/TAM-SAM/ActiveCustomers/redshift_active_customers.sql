WITH numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
deployments_base AS (
    SELECT 
        d.customer_id,
        d.deployments_id,
        d.deployments_name,
        d.product_area AS product_area_raw,
        d.deployment_phase,
        d.overall_status,
        d.production_flag,
        CASE 
            WHEN d.product_area IS NULL OR d.product_area = '' THEN 1
            ELSE REGEXP_COUNT(d.product_area, ';') + 1 
        END AS product_area_count
    FROM ccr_data_hub.bv_deployments d
    WHERE d.current_flag = 'Y'
),
deployments_split AS (
    SELECT 
        db.customer_id,
        db.deployments_id,
        db.deployments_name,
        db.product_area_raw,
        TRIM(SPLIT_PART(db.product_area_raw, ';', n.n)) AS new_product_area,
        db.deployment_phase,
        db.overall_status,
        db.production_flag
    FROM deployments_base db
    CROSS JOIN numbers n
    WHERE (n.n <= db.product_area_count
           AND TRIM(SPLIT_PART(db.product_area_raw, ';', n.n)) != '')
       OR (db.product_area_raw IS NULL AND n.n = 1)
)
SELECT DISTINCT
    s.account_id,
    s.customer_tenant_prefix,
    s.deployment_status,
    s.initial_go_live_date,
    ds.customer_id,
    ds.deployments_id,
    ds.deployments_name,
    ds.product_area_raw,
    ds.new_product_area,
    ds.deployment_phase,
    ds.overall_status,
    ds.production_flag
FROM ccr_data_hub.bv_customer_entitled_sku s
LEFT JOIN deployments_split ds
    ON LEFT(s.account_id, 15) = ds.customer_id
WHERE
    s.customer_tenant_prefix IS NOT NULL
    AND s.deployment_status = 'Deployed'
    AND s.initial_go_live_date IS NOT NULL
ORDER BY s.account_id, ds.deployments_id