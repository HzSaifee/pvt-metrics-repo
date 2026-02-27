SELECT DISTINCT
    d.sf_deployment_id,
    d.product_area AS old_product_area
FROM dw.lookup_db.sfdc_deployments d
WHERE d.product_area IS NOT NULL 
  AND TRIM(d.product_area) != ''