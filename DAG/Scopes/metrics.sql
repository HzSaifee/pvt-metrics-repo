SELECT
    tenant_name,
    wd_env,
    scope_selection_type,
    common_request_id,
    scope_id,
    wd_event_date
FROM
    swh.scopes_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
