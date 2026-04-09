SELECT
    wd_env,
    scope_id,
    validation_type,
    count_of_critical_errors,
    warnings_included,
    count_of_warnings,
    wd_event_date
FROM
    goku.scopes_validation_usages_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
