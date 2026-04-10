SELECT time, elapsed_time, migration_success_rate, migrated_error_count, total_instance_count,
       recipe_name, build_type, build_status, wd_env, customer_tenant, recipe_execution_tags
FROM {{ swh_table_name }}
WHERE wd_event_date IS NOT NULL
    AND CAST(wd_event_date AS TIMESTAMP) >= CAST('{{ oldest_date }}' AS TIMESTAMP)
    AND build_type = 'Foundation Tenant Build'
ORDER BY wd_event_date DESC
