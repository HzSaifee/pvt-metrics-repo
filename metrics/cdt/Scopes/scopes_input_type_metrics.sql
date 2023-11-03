SELECT
    wd_env,
    scope_id,
    input_type,
    wd_event_date
FROM
    swh.scopes_input_type_metrics
WHERE
    wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST(OLDEST_MONTH_VALUE_TO_SET AS TIMESTAMP) -- OLDEST_MONTH_VALUE_TO_SET to be set in the Python Script
