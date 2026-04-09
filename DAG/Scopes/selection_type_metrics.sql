SELECT
    exclude_count,
    include_count,
    wd_env,
    scope_id,
    impl_type,
    all_instances,
    exclude_all_instances,
    wd_event_date
FROM
    goku.scopes_selection_type_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_month_value }}' AS TIMESTAMP) -- oldest_month_value to be set in the Python Script
