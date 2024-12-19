SELECT wd_event_date, elapsed_time, migrated_error_count, total_instance_count,
       recipe_name, build_type, build_status, wd_env
FROM MAIN_TABLE_NAME_TO_SET
WHERE wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST(OLDEST_DATE_TO_SET AS TIMESTAMP)
    AND build_type = 'Foundation Tenant Build'
ORDER BY wd_event_date DESC