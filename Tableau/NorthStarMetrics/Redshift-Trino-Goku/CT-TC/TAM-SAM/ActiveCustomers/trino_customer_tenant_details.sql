SELECT DISTINCT
    scat.sf_account_id,
    scat.billing_id,
    scat.tenant_prefix,
    sct.status AS tenant_status,
    sct.tenant_type,
    sct.tenant_start_date,
    sct.tenant_expire_date

FROM dw.lookup_db.sfdc_customer_tenants sct
INNER JOIN dw.lookup_db.sfdc_customer_account_tenants scat
    ON sct.sf_id = scat.sf_account_id
WHERE scat.tenant_prefix IS NOT NULL
  AND TRIM(scat.tenant_prefix) != ''