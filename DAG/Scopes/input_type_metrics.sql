SELECT
    wd_env,
    scope_id,
    input_type,
    wd_event_date
FROM
    swh.scopes_input_type_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
