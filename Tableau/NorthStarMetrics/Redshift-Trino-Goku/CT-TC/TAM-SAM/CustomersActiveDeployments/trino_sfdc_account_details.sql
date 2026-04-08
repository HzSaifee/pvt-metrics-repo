SELECT DISTINCT
    sad.sf_account_id,
    COALESCE(sad.enterprise_size_group, 'Unknown') AS enterprise_size_group,
    COALESCE(sad.segment, 'Unknown')               AS segment,
    COALESCE(sad.super_industry, 'Unknown')         AS super_industry,
    COALESCE(sad.industry, 'Unknown')               AS industry

FROM dw.lookup_db.sfdc_account_details sad
WHERE sad.sf_account_id IS NOT NULL
  AND TRIM(sad.sf_account_id) != ''