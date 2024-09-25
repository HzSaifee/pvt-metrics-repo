SELECT recipe_name, target_tenant, customer_billing_id, ARRAY_AGG(DISTINCT recipe_execution_tags) AS recipe_tags
FROM swh.tenant_build
WHERE wd_event_date IS NOT NULL
 AND
    build_type = 'Foundation Tenant Build'
GROUP BY recipe_name, target_tenant, customer_billing_id