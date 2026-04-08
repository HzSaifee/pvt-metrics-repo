WITH numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
deployments_base AS (
    SELECT 
        d.customer_id,
        d.deployments_id,
        d.deployments_name,
        d.deployment_partner_name,
        d.deployment_type,
        d.product_area AS product_area_raw,
        d.overall_status,
        d.deployment_phase,
        d.deployment_start_date,
        CASE 
            WHEN d.product_area IS NULL OR d.product_area = '' THEN 1
            ELSE REGEXP_COUNT(d.product_area, ';') + 1 
        END AS product_area_count
    FROM ccr_data_hub.bv_deployments d
    WHERE d.current_flag = 'Y'
)
SELECT DISTINCT
    s.account_id,
    db.customer_id,
    db.deployments_id,
    db.deployments_name,
    db.deployment_partner_name,
    db.deployment_type,
    db.product_area_raw,
    TRIM(SPLIT_PART(db.product_area_raw, ';', n.n)) AS new_product_area,
    db.overall_status,
    db.deployment_phase,
    db.deployment_start_date
FROM deployments_base db
CROSS JOIN numbers n
LEFT JOIN ccr_data_hub.bv_customer_entitled_sku s
    ON LEFT(s.account_id, 15) = db.customer_id
WHERE n.n <= db.product_area_count
  AND TRIM(SPLIT_PART(db.product_area_raw, ';', n.n)) != ''
  OR (db.product_area_raw IS NULL AND n.n = 1)
ORDER BY db.customer_id, db.deployments_id, n.n