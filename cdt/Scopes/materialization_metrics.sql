SELECT
    wd_env,
    scope_id,
    implementation_type_count,
    total_instances_count,
    wd_event_date
FROM
    goku.scopes_materialization_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST(OLDEST_MONTH_VALUE_TO_SET AS TIMESTAMP) -- OLDEST_MONTH_VALUE_TO_SET to be set in the Python Script
